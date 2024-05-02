import argparse
import logging
import os
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine, text, URL
import subprocess
import shutil
from zipfile import ZipFile
import time
import re
import csv

###MODIFY VARIABLES BEFORE RUNNING###
IMPORT_FILES_DIRECTORY = "<import files directory>"
IMPORT_FILES = ["<file one>", "<file two>"]
EXPORT_FILE_PATH = "<export file path>"
EXPORT_FILE_UNC = "<export file network>"
EXPORT_FILE_DIR = "<export file directory>"
SQL_QUERY = sqlalchemy.text("""<sql query for export>""")
###################################

DELIMITER_COMMA = ','
DELIMITER_PIPE = '|'
DELIMITER_TAB = '\t'
HEADER_SHOW = True
HEADER_HIDE = False
OUTPUTS_REQUIRED = True
OUTPUTS_NONE = False


EventStatus = "START"
EventLogIDs = {}
timestr = time.strftime("%Y%m%d")

log_filename = f'Log/SSISIntegration_{timestr}.log'
logging.basicConfig(filename=log_filename, encoding='utf-8', level=logging.DEBUG, format=f'%(asctime)s {EventStatus}: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

logging.info(f"Starting new Event. Event status: {EventStatus}")

def connect_to_database(database_name):
    try:
        sql_url = f'mssql+pyodbc://DISD-SQL/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        logging.debug(f'Trying to connect to {database_name} database...')
        engine = sqlalchemy.create_engine(sql_url)
        connection = engine.connect()
        return connection
    except OSError as error:
        logging.error(f"ERROR: {error}")
        return None
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
    """given a directory, search path for files and import them: 
    * directory - directory to search"""
    try:
        for filename in os.listdir(directory):
            importFile(directory, filename)
    except OSError as error:
        logging.error(f"Error while listing files in directory {directory}: {error}")

def importFilesByFilePath(filepath, filenames):
    try:
        for file in filenames:
            importFile(filepath, file)
    except Exception as error:
        logging.exception(f"Error occurred while importing files: {error}")

def importFile(filepath, filename):
    """given a file path and the filename, check if exists and is compatible to run process: 
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
    """given a file path and the filename, import the csv file into the connected database with log info and stored procedures: 
    * filepath - full path to csv file
    * filename - name of the specific file"""
    logging.debug(f"START {filename}")
    try:
        data_df = pd.read_csv(filepath)
        db_schema = re.split('[._]', filename)
        
        if db_schema[0] == "AD" and (db_schema[1] == "Employee" or db_schema[1] == "Student"):
            EventLogID = execProcedure(connection_stage.engine, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", OUTPUTS_REQUIRED, params=['Import',  filename, EventStatus])
            EventLogIDs[filename] =  EventLogID     
            EventStatus = "STAGE"
            execProcedure(connection_stage.engine, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", OUTPUTS_NONE, params=[EventLogIDs[filename], EventStatus])
            try:
                table_name = db_schema[1]
                rowcount = data_df.to_sql(table_name, connection_stage.engine, schema="ad", index=False, if_exists="replace")
                logging.info(f"{len(data_df)} rows successfully imported to table {db_schema[0]}.{db_schema[1]}")
            except Exception as error:
                logging.debug(f"Error while importing data to table ad.{db_schema[1]}: {error}")
    except:
        logging.error(f"Error while importing CSV file {filename}: {error}")
    
    logging.debug(f"OK")

def unzipFiles(filepath, filename):
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
    try:
        EventStatus = "START"
        logging.debug(f"{EventStatus}: {filename}")

        base_filename = os.path.basename(filename)
        if base_filename not in EventLogIDs:
            EventLogID = execProcedure(connection_ods.engine, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", OUTPUTS_REQUIRED, params=['Export', base_filename, EventStatus])
            EventLogIDs[base_filename] = EventLogID
        
        EventStatus = "EXPORT"
        logging.debug(f"{EventStatus}: {filename}")

        user_df = pd.read_sql(query, connection)
        logging.info(f"Rows found: {user_df.shape[0]}")
        logging.info(user_df.head())

        user_df.to_csv(path_or_buf=filename, sep=csvDelimiter, header=includeHeader, index=False, mode='w', quoting=quoteOption)
        logging.info(f"Wrote CSV file '{filename}' with '{csvDelimiter}' delimiter, headers: {includeHeader}")

        execProcedure(connection_ods.engine, "EXEC dbo.usp_EventLogDetail_Insert @EventLogID = ?, @DetailName = ?, @DetailValue = ?;", OUTPUTS_NONE, params=[EventLogIDs[base_filename], base_filename, len(user_df)])

        EventStatus = "OK"
        execProcedure(connection_ods.engine, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", OUTPUTS_NONE, params=[EventLogIDs[base_filename], EventStatus])
    
    except Exception as error:
        logging.error(f"ERROR at {EventStatus}: {error}")

def execProcedure(engine: sqlalchemy.engine.Engine, procedure: str, outputs: bool, params: list | None = None):
    """given a stored procedure name and parameters, execute the SQL stored procedure against the connected database with parameters and return the results: 
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
    """Send a file to an archive with a date
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
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='type', required=True)
        

    parser_import = subparsers.add_parser('Import')
    parser_export = subparsers.add_parser('Export')
    
    parser_import.add_argument('-d, --directory', required=True)
    parser_import.add_argument("-f", "--files", nargs='+')

    parser_export.add_argument("-e, --export", required=True)
    parser_export.add_argument("-d", "--delimiter", choices=[',','|','\t'], default=',')
    parser_export.add_argument("-s", '--show', action='store_true')
    parser_export.add_argument("-q", "--query", required=True)
    parser_export.add_argument("-n", "--network")

    main_args = parser.parse_args()
    print(main_args)

    try: 
        if main_args.type.casefold() == 'Import'.casefold():
            IMPORT_FILES_DIRECTORY = main_args.directory
            if main_args.files:
                IMPORT_FILES =  main_args.files
                importFilesByFilePath(IMPORT_FILES_DIRECTORY, IMPORT_FILES)
            else:
                importFilesByDirectory(IMPORT_FILES_DIRECTORY)
        elif main_args.type.casefold() == 'Export'.casefold():
            EXPORT_FILE_PATH =  main_args.export
            DELIMITER = main_args.delimiter
            SHOW_HEADER = main_args.show
            SQL_QUERY = main_args.query
            if main_args.network:
                EXPORT_FILE_UNC = main_args.network
                copyToUNC(EXPORT_FILE_UNC, EXPORT_FILE_PATH, EXPORT_FILE_DIR)
            else:
                exportFile(EXPORT_FILE_PATH, DELIMITER, SHOW_HEADER, csv.QUOTE_ALL, SQL_QUERY, connection_ods)        
        logging.debug("Process completed successfully.")
    
    except:
        logging.error(f"Error occurred: {error}")
