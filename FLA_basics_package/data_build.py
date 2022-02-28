import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup, SoupStrainer
import zipfile
import wget
import os
import requests
from datetime import datetime
########   
#Data retrive / Data build Functions
########
def get_census_shp(fips=False, Geography=None, year=datetime.now().year-1):
    if year < 2008:
        raise Exception('Pre 2008 Tiger Files either do not existis or are not in common formate')
    stateList = pd.read_csv('us-state-ansi-fips.csv', dtype=str, skipinitialspace=True)
    base = f'https://www2.census.gov/geo/tiger/TIGER{year}/'
    if Geography is None:
        res = requests.get(base)
        res = res.content
        dir_list = []
        for link in BeautifulSoup(res, parse_only=SoupStrainer('a'), features="html.parser"):
            if not link.has_attr("class"):
                if 'https:' not in link.get("href") and '?' not in link.get("href") and (len(link.get("href").split('/')) == 2):
                    dir_list.append(link.get("href").replace('/',''))
        return print(f'Please Choice a Geography/Subdirectory you would like the shapefile for/from', *dir_list, sep='\n')
                
    elif not fips:
        raise Exception('Please Include State')
    sub = f'{Geography}/'
    url = base+sub
    res = requests.get(url)
    res = res.content
    zip_list = []
    for link in BeautifulSoup(res, parse_only=SoupStrainer('a'), features="html.parser"):
        if not link.has_attr("class"):
            zip_list.append(link.get("href"))
    try:
        int(fips)
    except:
        if len(fips) == 2:
            fips = stateList[stateList['stusps'] == fips.upper()]
            fips = fips['st'].values[0]
        elif len(fips) > 2:
            fips = stateList[stateList['stname'] == fips.capitalize()]
            fips = fips['st'].values[0]
    for i in zip_list:
        if fips in i.split('_') or 'us' in i.split('_'):
            gdf = gpd.read_file(f'{url}{i}')
            return gdf




def assign_baf(baf, state, geoid=None, disid=None):
    if not isinstance(baf, gpd.GeoDataFrame) and (geoid is None or  len(baf[geoid].iloc[0]) != 15):
        raise Exception('Need to include 15 digit GEOID for joiner')
    if isinstance(baf, gpd.GeoDataFrame) and 'geometry' not in baf.columns:
        raise Exception('Geopandas DataFrame need Geometry Column')

    if isinstance(baf, gpd.GeoDataFrame):
        blk_df = get_census_shp(fips=state, Geography='TABBLOCK20')
        geoid = 'GEOID20'
        out_df = gpd.sjoin(baf,blk_df)
        r = [i for i in out_df if i not in [disid,geoid]]
        out_txt = out_df.drop(columns=r)
        out_txt.to_csv(f'simple_{state}_baf.csv', index=False)

    elif isinstance(baf, pd.DataFrame):
        blk_df = get_census_shp(fips=state, Geography='TABBLOCK20')
        blk_df = blk_df.rename(columns={'GEOID20':geoid})
        out_df = blk_df.merge(baf, on=geoid)
        out_shp = out_df[[disid,'geometry']]
        out_shp = out_shp.dissolve(disid)
        out_shp.to_file(f'geo_{state}_baf.shp')
    return out_df
        
