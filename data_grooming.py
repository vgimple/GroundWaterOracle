import click
import data_utilities as du
import zipfile as zf

from tqdm import tqdm


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
    for file in tqdm(zipContents.files, desc='importing files', unit=' files'):
        du.importFile(zipFile, file, dbLink)


# boilerplate main entry point
if __name__ == '__main__':
    cli()