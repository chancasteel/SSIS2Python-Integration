SQL Server Script

## Overview
The SSIS2Python script is designed to facilitate various database operations, such as importing CSV files into a database, exporting query results to a CSV file, and copying files to a network location. The script leverages SQLAlchemy for database connections, Pandas for data manipulation, and several other Python libraries for file handling and logging.

## Prerequisites
Before running the script, ensure the following:
- Python 3.x is installed on your system.
- Required Python libraries are installed. You can install the dependencies using:
  ```bash
  pip install pandas sqlalchemy pyodbc
  ```
- A valid configuration file (`config.ini`) is present in the specified path with appropriate settings.

## Configuration File
It should include the necessary configuration settings, particularly the `EventLogIds` section. Below is an example of what the `config.ini` file might look like:

### Example `config.ini` File

```ini
[General]
Log = Log/SSISIntegration.log
Server = DISD-SQL

[EventLogIds]
; Example entries
file1.csv = 1
file2.csv = 2
```

## Reading the Configuration File
The script reads the configuration file to retrieve the necessary settings. Here is the relevant portion of the script that handles this:

```python
from configparser import ConfigParser

cf = ConfigParser()
cf.read('config.ini')

LOG_PATH = cf.get('General', 'Log')
SQL_SERVER = cf.get('General', 'Server')

EVENT_LOG_IDS = cf.get('EventLogIds')
```

## Script Usage
The script can be executed from the command line with different subcommands for importing, exporting, and copying files. Below are the details of the subcommands and their options.

### Import Subcommand
Imports CSV files from a specified directory into a database.

```bash
python script.py Import -d <directory> [-f <file1> <file2> ...] [--schema <schema>] [--table <table>] [--exists <replace|append>]
```

- `-d, --directory`: Directory path from which to import files.
- `-f, --files`: Specific filenames to import (optional).
- `--schema`: Schema name for import (optional).
- `--table`: Table name for import (optional).
- `--exists`: What to do if the table exists (`replace` or `append`, default is `replace`).

### Export Subcommand
Exports the result of an SQL query to a CSV file.

```bash
python script.py Export -e <export_path> -q <query> [-del <delimiter>] [-s]
```

- `-e, --export`: Path of the file to export.
- `-q, --query`: SQL query to export.
- `-del, --delimiter`: Delimiter to use for CSV (default is `,`).
- `-s, --show`: Whether to include the header in the exported CSV.

### Copy Subcommand
Copies a file to another folder or directory, optionally to a network location.

```bash
python script.py Copy -src <source_path> -dest <destination_path> [-n <network_path>]
```

- `-src, --source`: Source file path.
- `-dest, --destination`: Destination directory path.
- `-n, --network`: Path of the network/UNC (optional).

## Functions
The script includes several functions to handle the operations:

- `connect_to_database(database_name)`: Connects to a specified database.
- `importFilesByDirectory(directory)`: Imports files from a given directory.
- `importFilesByFilePath(filepath, filenames)`: Imports specific files from a directory.
- `importFile(filepath, filename)`: Imports a single file.
- `importCSV(filepath, filename)`: Imports a CSV file into the database.
- `unzipFiles(filepath, filename)`: Unzips files and imports them.
- `exportFile(filename, csvDelimiter, includeHeader, quoteOption, query, connection)`: Exports query results to a CSV file.
- `execProcedure(engine, procedure, outputs, params)`: Executes a stored procedure.
- `copyToUNC(network, src, dest, user, password)`: Copies files to a network location with optional authentication.
- `archiveFile(file, archive)`: Archives files by appending the current date to the filename.
- `update_config_file(section, config_data)`: Updates a specific section of the config file.

## Logging
The script logs its operations to a log file named `SSISIntegration_<YYYYMMDD>.log` located in the `Log` directory. Logs include detailed information about the execution status and any errors encountered.

## Running the Script
To run the script, execute the following command based on the desired operation:

### Example: Import Files
```bash
python script.py Import -d "C:\path\to\directory" --schema mySchema --table myTable --exists replace
```

### Example: Export Query Results
```bash
python script.py Export -e "C:\path\to\export.csv" -q "SELECT * FROM myTable"
```

### Example: Copy File to Network Location
```bash
python script.py Copy -src "C:\path\to\file.txt" -dest "\\network\path\to\destination"
```

Ensure you have the necessary permissions and configurations set up for the operations you intend to perform.
