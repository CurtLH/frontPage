import click
from frontpage import scraper
from frontpage import etl_process

@click.group()
def cli():

    """
    Web Crawler and ETL Process
    """

    pass


cli.add_command(scraper.cli, 'scraper')
cli.add_command(etl_process.cli, 'etl')
