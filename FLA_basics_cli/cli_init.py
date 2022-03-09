import click
import os
import FLABasicTools
from tabulate import tabulate
import pandas as pd
import geopandas as gpd
     
@click.command()
@click.option('--program', '-t', prompt='Choice Process', type=click.Choice(['assign_baf', 'community_split', 'overlap_compare']))
@click.option('--path',prompt='Path to working Directory', default=os.getcwd(), type=click.Path(exists=True))
def main(path, program):
    os.chdir(path)
    if program == 'assign_baf':
        df = click.prompt('Please enter File', type=str)
        if 'csv' in df.split('.'):
            csv = True
            table_sample = pd.read_csv(f'{df}',nrows=2)
            table = pd.read_csv(f'{df}',dtype=str)
        elif 'shp' in df.split('.'):
            table_sample = gpd.read_file(f'{df}',nrows=2)
            table = gpd.read_file(f'{df}',dtype=str)
        else:
            raise Exception("Please Supply a CSV or SHP file")
        click.echo(tabulate(table_sample, headers='keys', tablefmt='psql',showindex=False))
        click.confirm('Do you want to continue?', abort=True)
        district = click.prompt('Please enter Name of District Column', type=str)
        if csv:
            geoid = click.prompt('Please enter Name of 15 digit GEOID', type=str)
        else:
            geoid=None
        state = click.prompt('Please enter State for the Poltical Geography', type=str)
        FLABasicTools.assign_baf(table, state, district, geoid)
    if program == 'community_split':
        df = click.prompt('Please enter CSV', type=str)
        table_sample = pd.read_csv(f'{df}',nrows=2)
        table = pd.read_csv(f'{df}',dtype=str)
        click.echo(tabulate(table_sample, headers='keys', tablefmt='psql',showindex=False))
        click.confirm('Do you want to continue?', abort=True)
        district = click.prompt('Please enter Name of District Column', type=str)
        geoid = click.prompt('Please enter Name of 15 digit GEOID', type=str)
        FLABasicTools.community_split(distr=table, geoid=geoid, disid=district)
    if program == 'community_split':
        df = click.prompt('Please enter CSV', type=str)
        table_sample = pd.read_csv(f'{df}',nrows=2)
        table = pd.read_csv(f'{df}',dtype=str)
        click.echo(tabulate(table_sample, headers='keys', tablefmt='psql',showindex=False))
        click.confirm('Do you want to continue?', abort=True)
        district = click.prompt('Please enter Name of District Column', type=str)
        geoid = click.prompt('Please enter Name of 15 digit GEOID', type=str)
        leg = click.prompt('Please Choice Leg from List', type=click.Choice(['CD116', 'SLDU18', 'SLDL18']))
        FLABasicTools.Overlap_old_new(new_districts=table, geoid=geoid, district=district, leg=leg)
if __name__ == '__main__':
    main()