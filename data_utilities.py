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


def importFile(zipFile: zf.ZipFile, path: str, dbLink: DatabaseLink):
    """
    Import a file into the database.
    zipFile: zf.ZipFile    The zip file to import.
    pattern: str    The regular expression pattern to match the file names.
    dbLink: DatabaseLink    The database link.
    """
    pattern_full = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<station>\d*)_beginn_bis_(?P<date>\d\d\.\d\d\.\d\d\d\d)_.*\.csv'
    pattern_ytd = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<stelle>\d*)_(?P<start_date>\d\d\.\d\d\.\d\d\d\d)_(?P<end_date>\d\d\.\d\d\.\d\d\d\d)_.*\.csv'
    pattern_today = r'(?P<type>grundwasser-gwo|meteo-n)/(?P<station>\d*)_(?P<date>\d\d\.\d\d\.\d\d\d\d)_\D+.*\.csv'

    # files from the day that the data set was downloaded will be ignored as they are likely to contain no data
    if re.match(pattern=pattern_today, string=path):
        return


    pass
