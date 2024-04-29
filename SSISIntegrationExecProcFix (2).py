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

"""Declare constants"""
DELIMITER_COMMA = ','
DELIMITER_PIPE = '|'
DELIMITER_TAB = '\t'
HEADER_SHOW = True
HEADER_HIDE = False


EventStatus = ""
EventLogIDs = {}
timestr = time.strftime("%Y%m%d")
logging.basicConfig(filename=f'Log/SSISIntegration_{timestr}.log', encoding='utf-8', level=logging.DEBUG, format=f'%(asctime)s {EventStatus}: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
""" Use a trusted connection to connect to a database """
try:    
    EventStatus = "START"
    sqlStage_url = f'mssql+pyodbc://DISD-SQL/Stage?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    logging.debug('Trying to connect to ' + str(sqlStage_url))
    engineStage = sqlalchemy.create_engine(sqlStage_url)
    connectionStage = engineStage.connect()
    sqlOds_url = f'mssql+pyodbc://DISD-SQL/ODS?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    logging.debug('Trying to connect to ' + str(sqlOds_url))
    engineOds = sqlalchemy.create_engine(sqlOds_url)
    connectionOds = engineOds.connect()

except OSError as error:
    logging.error(f"ERROR at {EventStatus}: {error}")

def importFilesByDirectory(directory):
    """given a directory, search path for files and import them: 
    * directory - directory to search"""
    for filename in os.list(directory):
        importFile(directory, filename)

def importFilesByFilePath(filepath, filenames):
    """given a filepath and an array of filenames, search path for filename and import them: 
    * filepath - full path to file directory
    * filenames - array of specific file names"""
    for filename in filenames:
        importFile(filepath, filename)

def importFile(filepath, filename):
    """given a file path and the filename, check if exists and is compatible to run process: 
    * filepath - full path to file directory
    * filename - name of the specific file"""
    f = os.path.join(filepath, filename)
    file = Path(f)
    if file.exists() and file.is_file and file.suffix in [".csv", ".zip"]:
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
    EventStatus = "START"
    logging.debug(f"{EventStatus} {filename}")
    data_df = pd.read_csv(filepath)
    db_schema = re.split('[._]', filename)
    logging.debug(f"{EventStatus} {filename}")
    if db_schema[0] == "AD" and db_schema[1] == ["Employee", "Student"]:
        EventLogID = execProcedure(engineStage, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", params=['Import',  filename, EventStatus])
        EventLogIDs[filename] =  EventLogID     
        EventStatus = "STAGE"
        execProcedure(engineStage, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", params=[EventLogIDs[filename], EventStatus])
        try:
            table_name = db_schema[1]
            rowcount = data_df.to_sql(table_name, engineStage, schema="ad", index=False, if_exists="replace")
        except OSError as error:
            logging.debug(f"ERROR at {EventStatus}: {error}")
    logging.info(f"{len(data_df)} rows successfully imported to table {db_schema[0]}.{db_schema[1]}")
    EventStatus = "OK"
    logging.debug(f"{EventStatus}")

def unzipFiles(filepath, filename):
    try:
        EventStatus = "TRANSFORM"
        logging.debug(f"{EventStatus} {filename}")
        with ZipFile(filepath, 'r') as zObject:
            extract_path = filepath.rpartition('.')[0]
            zObject.extractall(path=extract_path)
            importFilesByDirectory(extract_path)
    except OSError as error:
        logging.debug(f"ERROR at {EventStatus}: {error}")
    logging.debug(f"{EventStatus}")


def exportFile(filename, csvDelimiter, includeHeader, quoteOption, query, connection):
    """given a filename and SQL query, run the SQL query against the connected database and export a CSV file to the provided filename
    parameters: 
    * filename - the file path and filename to be exported
    * csvDelimiter - the separator in the CSV file, usually a comma but sometimes a pipe or even a tab
    * includeHeader - whether to include (True) or not include headers (False)
    * quoteOption - whether to include double quotes around the values.  Options include:
        - csv.QUOTE_ALL - quote all fields
        - csv.QUOTE_MINIMAL - quote those fields which contain special characters such as delimiter, quotechar or any of the characters in lineterminator
        - csv.QUOTE_NONNUMERIC - quote all non-numeric fields.
        - csv.QUOTE_NONE - never quote fields
        - Most commonly used is csv.QUOTE_MINIMAL
    * query - the SQL query to be run
    * connection - the database connection to use for the query"""
    try:
        EventStatus = "START"
        logging.debug(f"{EventStatus} {filename}")
        execProcedure(engineOds, "DECLARE @EventLogID int; EXEC dbo.usp_EventLog_Insert @EventType=?, @EventSubtype=?, @EventStatus=?, @EventLogID = @EventLogID OUTPUT; select @EventLogID as EventLogID;", params=['Export',  filename, EventStatus])
        EventStatus = "EXPORT"
        logging.debug(f"{EventStatus} {filename}")
        user_df = pd.read_sql(query, connection)
        logging.info('Rows found: ' + str(user_df.shape[0]))
        logging.info(user_df.head())
        user_df.to_csv(path_or_buf=filename, sep=csvDelimiter, header=includeHeader, index=False, mode='w', quoting=quoteOption)
        logging.info(f"Wrote CSV file {filename} with '{csvDelimiter}' delimiter, headers: {includeHeader}")
        execProcedure(engineOds, "EXEC dbo.usp_EventLogDetail_Insert @EventLogID = ?, @DetailName = ?, @DetailValue = ?;", params=[EventLogIDs[filename], filename, len(user_df)])
        EventStatus = "OK"
        execProcedure(engineOds, "EXEC dbo.usp_EventLog_Update @EventLogID = ?, @EventStatus = ?;", params=[EventLogIDs[filename], EventStatus])
    except OSError as error:
        logging.error(f"ERROR at {EventStatus}: {error}")

def execProcedure(engine: sqlalchemy.engine.Engine, procedure: str, outputs: bool, params: list | None = None):
    """given a stored procedure name and parameters, execute the SQL stored procedure against the connected database with parameters and return the results: 
    * procedure - the name of the stored procedure to execute
    * outputs - whether the execution procedure returns a value
    * params () - an array of parameters 
    * engine - the database engine to use for the procedure"""
    logging.debug(f"Trying to execute procedure - {procedure} - with parameters - {params} in {engine.url.database} database")
    results = None
    try:
        with engine.raw_connection().cursor() as cursor:
            cursor.execute(text(procedure), params)
            if outputs:
                results = cursor.fetchone()
                logging.debug(f"Execution resulted in {results}")
    except Exception as error:
        logging.error(f"ERROR: {error}")
    return results[0] if results else None

    

def copyToUNC(network : os.PathLike, src: os.PathLike, dest: os.PathLike, user: str = None, password: str = None):
    """Copy a file to a network server
    * network -  the path of the network
    * src - the path of the source file to copy
    * dest - the path to the destination directory of folder to store copy
    * user (optional) - username for the host server
    * password (optional) - password for the user of the server """
    if user and password:
        winCMD = 'NET USE ' + network + ' /User:' + user + ' ' + password
        subprocess.Popen(winCMD, stdout = subprocess.PIPE, shell = True)
    copy_path  = shutil.copy2(src, network + dest)
    return copy_path

def archiveFile(file : os.PathLike, archive : os.PathLike):
    """Send a file to an archive with a date
    * file - the file to archive
    * archive - the path to archive directory or folder"""
    archiveFileName = file + timestr
    return shutil.copy2(archiveFileName, archive)


if __name__ == '__main__':
    """Import AD Employee and Student files"""
    importFilePath = "J:\\SQL DATA\\IMPORTS\\AD Import\\"
    importFiles = ["AD_Employee.csv", "AD_Student.csv"]
    importFilesByFilePath(importFilePath, importFiles)
    """Export CardsOnline Student file"""
    dbFilePath = "J:\\SQL DATA\\EXPORTS\\CardsOnline\\Duncanville_Student_IDs.csv"
    exportFileUnc = "\\\\TEC-TSK-01\\FTP\\"
    exportFileDir = "CardsOnline\\"
    sqlQuery = sqlalchemy.text("""SELECT [Student ID], [First Name], [Last Name], Grade, [Campus Code], [Campus Name] 
                               FROM skystu.vw_CardsOnline_Student 
                               ORDER BY [Student ID];""")
    exportFile(dbFilePath, DELIMITER_COMMA, HEADER_SHOW, csv.QUOTE_ALL, sqlQuery, connectionOds)
    copyToUNC(exportFileUnc, dbFilePath, exportFileDir)
    # engineOds.raw_connection().cursor().callproc