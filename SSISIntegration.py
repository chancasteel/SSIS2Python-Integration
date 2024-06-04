import argparse
import configparser
import csv
import logging
import os
import pandas as pd
from pathlib import Path
import sqlalchemy
import subprocess
import shutil
from zipfile import ZipFile
import time
import re

# Constants to define output requirements
OUTPUTS_REQUIRED = True
OUTPUTS_NONE = False

# Initial event status and log ID dictionary
EventStatus = "START"

#Initialize Config File
config = configparser.ConfigParser()
config.read('config.ini')
EventLogIDs = dict(config.items('EventLogIds'))


# Generate a timestamp string for log file naming
timestr = time.strftime("%Y%m%d")

# Initialize table and schema names as None
TABLE_NAME = None
SCHEMA_NAME = None

# Define the log filename based on the current date
log_filename = f'Log/SSISIntegration_{timestr}.log'

# Set up the logging configuration
logging.basicConfig(filename=log_filename, encoding='utf-8', level=logging.DEBUG, format=f'%(asctime)s {EventStatus}: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# Log the start of a new event
logging.info(f"Starting new Event. Event status: {EventStatus}")

def connect_to_database(database_name):
    """Function to connect to a database using SQLAlchemy"""
    try:
        sql_url = f'mssql+pyodbc://DISD-SQL/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        logging.debug(f'Trying to connect to {database_name} database...')
        engine = sqlalchemy.create_engine(sql_url)
        connection = engine.connect()
        return connection
    except OSError as error:
        logging.error(f"ERROR: {error}")
        return None

# Attempt to connect to the "Stage" and "ODS" databases
try:
    EventStatus = "START"
    connection_stage = connect_to_database("Stage")
    connection_ods = connect_to_database("ODS")

    if connection_stage and connection_ods:
        logging.info("Successfully connected to databases.")
    else:
        logging.error("Failed to connect to one or more databases.")
except Exception as error:
    logging.error(f"ERROR at {EventStatus}: {error}")

def importFilesByDirectory(directory : str):
    """Function to import files from a directory 
    * directory - directory to search"""
    try:
        for filename in os.listdir(directory):
            importFile(directory, filename,type='append')
    except OSError as error:
        logging.error(f"Error while listing files in directory {directory}: {error}")

def importFilesByFilePath(filepath, filenames, type=None):
    """Function to import specific files from a given directory 
    * filepath - directory to search
    * filenames - filenames to search for in directory"""
    try:
        for file in filenames:
            importFile(filepath, file)
    except Exception as error:
        logging.exception(f"Error occurred while importing files: {error}")

def importFile(filepath, filename):
    """Function to import a single file 
    * filepath - full path to file directory
    * filename - name of the specific file"""
    f = os.path.join(filepath, filename)
    file = Path(f)
    if file.exists() and file.is_file() and file.suffix in [".csv", ".zip"]:
        EventStatus = "STAGE"
        logging.debug(f"{EventStatus} {file}")
        if file.suffix == ".csv":
            importCSV(file, filename)
        elif file.suffix == ".zip":
            unzipFiles(file, filename)

def importCSV(filepath, filename):
    """Function to import a CSV file into the database 
    * filepath - full path to csv file
    * filename - name of the specific file"""
    logging.debug(f"START {filename}")
    try:
        if main_args.schema and main_args.table:
            schema_name = main_args.schema
            table_name = main_args.table
        else:
            db_schema = re.split('[._]', filename)
            table_name = db_schema[1]
            schema_name = db_schema[0]
        data_df = pd.read_csv(filepath)

        EventStatus = "START"
        EventLogID = execProcedure(connection_stage.engine, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", OUTPUTS_REQUIRED, params=['Import',  filename, EventStatus])
        EventLogIDs[filename] =  EventLogID
        update_config_file('EventLogIds', EventLogIDs)
          
        EventStatus = "STAGE"
        execProcedure(connection_stage.engine, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", OUTPUTS_NONE, params=[EventLogIDs[filename], EventStatus])
        try:
            rowcount = data_df.to_sql(table_name, connection_stage.engine, schema=schema_name.lower(), index=False, if_exists=main_args.exists)
            logging.info(f"{len(data_df)} rows successfully imported to table {schema_name}.{table_name}")
        except Exception as error:
            logging.debug(f"Error while importing data to table {schema_name}.{table_name}: {error}")
    except Exception as error:
        logging.error(f"Error while importing CSV file {filename}: {error}")
    
    logging.debug("OK")

def unzipFiles(filepath, filename):
    """Function to unzip files and import them"""
    try:
        EventStatus = "TRANSFORM"
        logging.debug(f"{EventStatus} {filename}")

        with ZipFile(filepath, 'r') as zObject:
            extract_path = os.path.splitext(filepath)[0]
            zObject.extractall(path=extract_path)
        
        importFilesByDirectory(extract_path)

        logging.debug(f"{EventStatus}")
    except Exception as error:
        EventStatus = "ERROR"
        logging.error(f"{EventStatus} at {filename}: {error}")
        logging.debug(f"{EventStatus}")

def exportFile(filename, csvDelimiter, includeHeader, quoteOption, query, connection):
    """Function to export a query result to a CSV file"""
    try:
        EventStatus = "START"
        logging.debug(f"{EventStatus}: {filename}")

        base_filename = os.path.basename(filename)
        if base_filename not in EventLogIDs:
            EventLogID = execProcedure(connection_ods.engine, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", OUTPUTS_REQUIRED, params=['Export', base_filename, EventStatus])
            EventLogIDs[base_filename] = EventLogID
            update_config_file('EventLogIDs', EventLogIDs)
        
        EventStatus = "EXPORT"
        logging.debug(f"{EventStatus}: {filename}")

        user_df = pd.read_sql(sqlalchemy.text(query), connection)
        logging.info(f"Rows found: {user_df.shape[0]}")
        logging.info(user_df.head())

        user_df.to_csv(path_or_buf=filename, sep=csvDelimiter, header=includeHeader, index=False, mode='w', quoting=quoteOption)
        logging.info(f"Wrote CSV file '{filename}' with '{csvDelimiter}' delimiter, headers: {includeHeader}")

        execProcedure(connection_ods.engine, "EXEC dbo.usp_EventLogDetail_Insert @EventLogID = ?, @DetailName = ?, @DetailValue = ?;", OUTPUTS_NONE, params=[EventLogIDs[base_filename], base_filename, len(user_df)])

        EventStatus = "OK"
        execProcedure(connection_ods.engine, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", OUTPUTS_NONE, params=[EventLogIDs[base_filename], EventStatus])
    
    except Exception as error:
        logging.error(f"ERROR at {EventStatus}: {error}")

def execProcedure(engine: sqlalchemy.engine.Engine, procedure: str, outputs: bool, params: list = None):
    """Function to execute a stored procedure with optional parameters and output 
    * procedure - the name of the stored procedure to execute
    * outputs - whether the execution procedure returns a value
    * params () - an array of parameters 
    * engine - the database engine to use for the procedure"""
    logging.debug(f"Trying to execute procedure '{procedure}' with parameters '{params}' in '{engine.url.database}' database")
    results = None
    try:
        with engine.raw_connection().cursor() as cursor:
            cursor.execute(procedure, params)
            if outputs:
                results = cursor.fetchone()
                logging.debug(f"Execution resulted in {results}")
    except Exception as error:
        logging.error(f"ERROR while executing procedure '{procedure}': {error}")
    return results[0] if results else None

def copyToUNC(network: os.PathLike, src: os.PathLike, dest: os.PathLike, user: str = None, password: str = None):
    """Function to copy files to a network location with optional authentication"""
    try:
        if user and password:
            winCMD = f'NET USE {network} /User:{user} {password}'
            subprocess.Popen(winCMD, stdout = subprocess.PIPE, shell = True)
        
        copy_path = shutil.copy2(src, os.path.join(network, dest))
        return copy_path
    except Exception as error:
        logging.error(f"Error copying file: {error}")
        return None
    
def archiveFile(file : os.PathLike, archive : os.PathLike):
    """Function to archive files by appending the current date to the filename
    * file - the file to archive
    * archive - the path to archive directory or folder"""
    try:
        current_date = time.strftime("%Y%m%d")

        file_name, file_extension = os.path.splitext(file)

        archive_file_name = f"{file_name}_{current_date}{file_extension}"

        archive_path = os.path.join(archive, os.path.basename(archive_file_name))
        shutil.copy2(file, archive_path)

        return archive_path
    except Exception as error:
        logging.error(f"Error archiving file: {error}")
        return None

def update_config_file(section, config_data):
    """Updates a specific section of the config.ini file with the provided config_data dictionary.

    Args:
        section: The name of the section to update in the config file.
        config_data: A dictionary containing key-value pairs for the specified section.
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')

        for key, value in config_data.items():
            config.set(section, key, str(value))

        with open('config.ini', 'w') as configfile:
            config.write(configfile)
    except Exception as error:
        logging.error(f"Error while updating config file section {section} with {config_data}: {error}")
    


def setup_arg_parser():
    """Function to set up the argument parser for command-line options"""

    parser = argparse.ArgumentParser(prog='SQL Server SSIS Process')
    subparsers = parser.add_subparsers(dest='type', required=True)

    # Subparser for the 'Import' operation
    parser_import = subparsers.add_parser('Import', help="Import csv file(s) to database")
    parser_import.add_argument('-d', '--directory', required=True, help="Directory path from which to import file(s)")
    parser_import.add_argument("-f", "--files", nargs='+', help="Specific filename(s) to import")
    parser_import.add_argument("--schema", dest='schema', help="Name of schema for import")
    parser_import.add_argument("--table", dest='table', help="Name of schema for import")
    parser_import.add_argument("--exists", dest='exists', choices={'replace', 'append'}, default='replace', help="What to do if table exists by default 'replace'")

    # Subparser for the 'Export' operation
    parser_export = subparsers.add_parser('Export', help="Export query from database to csv")
    parser_export.add_argument("-e", "--export", required=True, help="Path of file to export")
    parser_export.add_argument("-del", "--delimiter", choices=[',','|','\t'], default=',', help="Delimiter to use for csv")
    parser_export.add_argument("-s", '--show', action='store_true', help="Whether to show header")
    parser_export.add_argument("-q", "--query", required=True, help="SQL Query to export")

    # Subparser for the 'Copy' operation
    parser_copy = subparsers.add_parser('Copy', help="Send copy of file to another folder/directory")
    parser_copy.add_argument("-src", "--source", required=True, help="Source file path")
    parser_copy.add_argument("-dest", "--destination", required=True, help="Destination directory path")
    parser_copy.add_argument("-n", "--network", help="Path of network/unc")

    return parser.parse_args()

def import_files(directory, files=None):
    """Function to import files based on directory and specific files if provided"""
    if files:
        importFilesByFilePath(directory, files)
    else:
        importFilesByDirectory(directory)

def process_import(args):
    """Function to process the 'Import' command-line option"""
    IMPORT_FILES_DIRECTORY = args.directory
    IMPORT_FILES = args.files
    import_files(IMPORT_FILES_DIRECTORY, IMPORT_FILES)

def process_export(args):
    """Function to process the 'Export' command-line option"""
    EXPORT_FILE_PATH = args.export
    DELIMITER = args.delimiter
    SHOW_HEADER = args.show
    SQL_QUERY = args.query
    exportFile(EXPORT_FILE_PATH, DELIMITER, SHOW_HEADER, csv.QUOTE_ALL, SQL_QUERY, connection_ods)

def process_copy(args):
    """Function to process the 'Copy' command-line option"""
    SOURCE_FILE = args.source
    DEST_DIRECTORY = args.destination
    FILE_UNC = args.network
    copyToUNC(FILE_UNC, SOURCE_FILE, DEST_DIRECTORY)

# Main execution entry point
if __name__ == '__main__':
    main_args = setup_arg_parser()
    try:
        if main_args.type.casefold() == 'Import'.casefold():
            process_import(main_args)
        elif main_args.type.casefold() == 'Export'.casefold():
            process_export(main_args)
        elif main_args.type.casefold() == 'Copy'.casefold():
            process_copy(main_args)
        logging.debug("Process completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
