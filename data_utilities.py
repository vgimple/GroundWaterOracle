import re
import sqlite3
import zipfile as zf

from collections import namedtuple


"""
This module contains utility functions for working with the database.
"""


# Utility class for open database connections
class DatabaseLink:
    """
    A class to represent a link to a SQLite database.
    """
    def __init__(self, conn : sqlite3.Connection, cursor: sqlite3.Cursor):
        self.connection = conn
        self.cursor = cursor

    def __del__(self):
        self.connection.commit()
        self.connection.close()


# Prepare the database
def prepareDatabase(sqlitedb: str) -> DatabaseLink:
    """
    Prepare the database by creating the table if it does not exist.
    sqlitedb: str   Path to the SQLite database file.	    
    """
    # Create a connection to the database
    # (will create the database file if it does not exist yet)
    conn = sqlite3.connect(sqlitedb)
    cursor = conn.cursor()

    # Create the tables if they do not exist yet
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS water_data (
        date TEXT PRIMARY KEY
        -- Add N columns dynamically
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS water_stations (
        id TEXT PRIMARY KEY,
        name TEXT,
        offset REAL
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rain_data (
        date TEXT PRIMARY KEY
        -- Add N columns dynamically
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rain_stations (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    ''')

    return DatabaseLink(conn, cursor)


# Utility class for the contents of a zip file
class ZipFileContents:
    """
    A class to represent the contents of a zip file.
    """
    def __init__(self):
        self.files = []
        self.waterStations = []
        self.rainStations = []


# Get the contents of a zip file
def getZipContents(zipFile: zf.ZipFile) -> ZipFileContents:
    """
    Get the contents of a zip file.
    zipfile: str    Path to the zip file.
    """
    result = ZipFileContents()
    result.files.extend(zipFile.namelist())

    water_files = [f for f in result.files if f.startswith('grundwasser-gwo/')]
    meteo_files = [f for f in result.files if f.startswith('meteo-n/')]
    result.waterStations.extend(list(set([re.search(r'/(?P<number>\d+)_', f).group('number') for f in water_files])))
    result.rainStations.extend(list(set([re.search(r'/(?P<number>\d+)_', f).group('number') for f in meteo_files])))

    return result


# add a column to a table if it does not exist yet
def addColumnIfNotExist(dbLink: DatabaseLink, table: str, columnName: str):
    """
    Add a column to a table if it does not exist yet.
    dbLink: DataBaseLink    The database link.
    table: str    The table name.
    columnName: str    The column name.
    """
    # Check if the column exists
    dbLink.cursor.execute(f"PRAGMA table_info({table})")
    columns = dbLink.cursor.fetchall()
    column_exists = any(column[1] == columnName for column in columns)

    # Add the column if it doesn't exist
    if not column_exists:
        dbLink.cursor.execute(f"ALTER TABLE {table} ADD COLUMN '{columnName}' REAL")

    # Commit the changes and close the connection
    dbLink.connection.commit()  


def convertToFloat(string_value):
    if string_value == '':
        return None
    try:
        return float(string_value)
    except ValueError:
        return None
    

def importFile(zipFile: zf.ZipFile, path: str, dbLink: DatabaseLink):
    """
    Import a file into the database.
    zipFile: zf.ZipFile    The zip file to import.
    pattern: str    The regular expression pattern to match the file names.
    dbLink: DatabaseLink    The database link.
    """
    pattern_full = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<station>\d*)_beginn_bis_(?P<date>\d\d\.\d\d\.\d\d\d\d)_.*\.csv'
    pattern_ytd = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<station>\d*)_(?P<start_date>\d\d\.\d\d\.\d\d\d\d)_(?P<end_date>\d\d\.\d\d\.\d\d\d\d)_.*\.csv'
    pattern_today = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<station>\d*)_(?P<date>\d\d\.\d\d\.\d\d\d\d)_\D+.*\.csv'

    # files from the day that the data set was downloaded will be ignored as they are likely to contain no data
    if re.match(pattern=pattern_today, string=path):
        return

    match = re.match(pattern=pattern_full, string=path)
    if match is None:
        match = re.match(pattern=pattern_ytd, string=path)
    if match is None:
        raise RuntimeError(f"file {path} does not match any of the expected patterns")
    
    fileType = None
    if match.group('type') == 'grundwasser-gwo':
        fileType = 'water'
    elif match.group('type') == 'meteo-n':
        fileType = 'rain'
    if fileType is None:
        raise RuntimeError(f"file {path} seems to be neither water nor rain data")
    
    station = match.group('station')
    addColumnIfNotExist(dbLink, f'{fileType}_data', station)

    pattern_station_name = r'Messstellen-Name.;(?P<name>.*)'
    pattern_header = r'Datum;.*;Prüfstatus'
    with zipFile.open(path) as file:
        data_started = False
        for line in file:
            line = line.decode('utf-8')
            match = re.match(pattern=pattern_station_name, string=line) 
            if data_started:
                line = line.replace(',', '.')
                values = line.split(';')
                if len(values) < 2:
                    continue
                #sql = f"UPDATE {fileType}_data SET '{station}' = ? WHERE date = ?"
                sql = f"INSERT OR REPLACE INTO {fileType}_data (date, '{station}') VALUES (?, ?)"
                dbLink.cursor.execute(sql, (values[0], convertToFloat(values[1])))
                continue
            elif match:
                station_name = match.group('name')
                dbLink.cursor.execute(f"INSERT OR REPLACE INTO {fileType}_stations (id, name) VALUES (?, ?)", (station, station_name))                
                continue
            elif re.match(pattern=pattern_header, string=line):
                data_started = True
                continue
    dbLink.connection.commit()
