import click
import data_utilities as du
import os
import zipfile as zf

from tqdm import tqdm


# TODOs:
# - make creating a new sqlite database an option of the zipimport command
# - expand backup functionality to more than just one backup
# - import at the moment is horribly slow - it would probably be better to 
#   build the data in memory and then commit it to the database


# container to host global options
class GlobalOptions:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    def add_attribute(self, name, value):
        setattr(self, name, value)
global_options = GlobalOptions()


# global options
@click.group()
@click.option('--sqlitedb', type=click.Path(), required=True, help="Path to the sqlite database file on which to operate")
def cli(sqlitedb):
    click.echo(f"using database file {sqlitedb}...")
    global_options.add_attribute('sqlitedb', sqlitedb)
    if os.path.exists(sqlitedb):
        click.echo("database file exists - creating backup...")
        if os.path.exists(sqlitedb + '.bak'):
            os.remove(sqlitedb + '.bak')
        os.rename(sqlitedb, sqlitedb + '.bak')


# import downloaded data into the database
@cli.command()
@click.option('--zipfile', prompt='zip file to import', help='the downloaded zip file from the "Bayerisches Landesamt für Umwelt" to import')
def zipimport(zipfile):
    # prepare access and gather stats
    dbLink = du.prepareDatabase(global_options.sqlitedb)
    zipFile = zf.ZipFile(zipfile, 'r')
    zipContents = du.getZipContents(zipFile)
    click.echo(f"zip file {zipfile} contains data from {len(zipContents.rainStations)} rain measurement stations and {len(zipContents.waterStations)} water measurement stations...")

    # import data
    for file in tqdm(zipContents.files, desc='importing files', unit='file'):
        du.importFile(zipFile, file, dbLink)


# boilerplate main entry point
if __name__ == '__main__':
    cli()