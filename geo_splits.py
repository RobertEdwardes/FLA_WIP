import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup, SoupStrainer
import zipfile
import wget
import os
import requests
from datetime import datetime
import json
########
#Helper Functions
########
def get_links(url):
    url_base = 'https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/'
    res = requests.get(url)
    res = res.content
    array = []
    f= None 
    for link in BeautifulSoup(res, parse_only=SoupStrainer('a')):
        if link.has_attr('href') and '.zip'  in link['href']:
            array.append(link['href'])
    return array
########   
#Data retrive / Data build Functions
########
# def get_census_shp(fips=False, Geography=None, year=datetime.now().year-1):
#     if year < 2008:
#         raise Exception('Pre 2008 Tiger Files either do not existis or are not in common formate')
#     stateList = pd.read_csv('https://gist.githubusercontent.com/dantonnoriega/bf1acd2290e15b91e6710b6fd3be0a53/raw/11d15233327c8080c9646c7e1f23052659db251d/us-state-ansi-fips.csv', dtype=str, skipinitialspace=True)
#     base = f'https://www2.census.gov/geo/tiger/TIGER{year}/'
#     if Geography is None:
#         res = requests.get(base)
#         res = res.content
#         dir_list = []
#         for link in BeautifulSoup(res, parse_only=SoupStrainer('a'), features="html.parser"):
#             if not link.has_attr("class"):
#                 if 'https:' not in link.get("href") and '?' not in link.get("href") and (len(link.get("href").split('/')) == 2):
#                     dir_list.append(link.get("href").replace('/',''))
#         return print(f'Please Choice a Geography/Subdirectory you would like the shapefile for/from', *dir_list, sep='\n')
                
#     elif not fips:
#         raise Exception('Please Include State')
#     sub = f'{Geography}/'
#     url = base+sub
#     res = requests.get(url)
#     res = res.content
#     zip_list = []
#     for link in BeautifulSoup(res, parse_only=SoupStrainer('a'), features="html.parser"):
#         if not link.has_attr("class"):
#             zip_list.append(link.get("href"))
#     try:
#         int(fips)
#     except:
#         if len(fips) == 2:
#             fips = stateList[stateList['stusps'] == fips.upper()]
#             fips = fips['st'].values[0]
#         elif len(fips) > 2:
#             fips = stateList[stateList['stname'] == fips.capitalize()]
#             fips = fips['st'].values[0]
#     for i in zip_list:
#         if fips in i.split('_') or 'us' in i.split('_'):
#             gdf = gpd.read_file(f'{url}{i}')
#             return gdf
# def assign_baf(shp, state, id_col=None,  clean=False, point=False):
#     r = [id_col,'GEOID20']
#     if not isinstance(shp, gpd.GeoDataFrame):
#         raise Exception('Need to pass GeoDataFrame')
#     if clean and (not isinstance(id_cols, list) or len(id_cols) != 2):
#         raise Exception('Needs 2 columns to keep in (id_cols)') 
#     blk_df = get_census_shp(fips=state, Geography='TABBLOCK20')
#     if id_col == 'GEOID20':
#         blk_df = blk_df.rename(columns={'GEOID20':'TABBLOCK20'})
#         r = [id_col,'TABBLOCK20']
#     if point:
#             blk_df['geometry'] = gpd.points_from_xy(blk_df.INTPTLON20, blk_df.INTPTLAT20)
#     if not clean:
#         out_df = gpd.sjoin(shp,blk_df)
#     elif clean:
#         out_df = gpd.sjoin(shp,blk_df)
#         for i in out_df.columns:
#             if i not in id_cols:
#                 r.append(i)
#         out_df = out_df.remove(columns=r)
#
#    return out_df
########
#Split Functions
########


def community_split(distr, geoid, disid):
    if not isinstance(distr, pd.DataFrame):
        raise Exception('distr is not a Dataframe')
    url_base = 'https://www2.census.gov/geo/tiger/TIGER2020/'
    f = None 
    if len(distr.columns) != 2:
        raise Exception('Please clean Dataframe so only 2 columns GEOID and District ID are included')
    if len(distr[geoid].iloc[0]) < 15:
        raise Exception('GEOID column not in 15 digital Geocode for Census Blocks')
    state = distr[geoid].iloc[0][:2]
    links = get_links(f'{url_base}TABBLOCK20') 
    for l in links:
        if state in l.split('_')[2]:
            f = l
            break
    wget.download(f'{url_base}/TABBLOCK20/{f}')
    tabblock = gpd.read_file(f, dtype={'GEOID20': str})
    distr = distr.rename(columns={geoid:'GEOID20'})
    df_dist = tabblock.merge(distr, on='GEOID20')
    os.remove(f)
    if len(df_dist.index) == 0:
        raise Exception('Pandas Merge Fail')
    #County Split
    keep=[disid,'COUNTYFP20']
    remove=[]
    for i in df_dist.columns:
        if i not in keep:
            remove.append(i)
    df_dist = df_dist.drop(columns=remove)
    district_list = list(set(df_dist['COUNTYFP20'].values.tolist()))
    pivot_tbd = {}
    for i in district_list:
        pivot_tbd[i] = []
    for idx, row in df_dist.iterrows():
        pivot_tbd[row['COUNTYFP20']].append(row[disid])
        pivot_tbd[row['COUNTYFP20']] = list(set(pivot_tbd[row['COUNTYFP20']]))
    r = []
    for key, value in pivot_tbd.items():
        if len(value) < 2:
            r.append(pivot_tbd[key])
    Segement_Count =  0
    Split_Count = 0
    for key,value in pivot_tbd.items():
        if len(value) > 1:
            Segement_Count += len(value)
            Split_Count += 1
    out_county = {'Segement_Count':Segement_Count, 'Split_Count':Split_Count, 'County_List': pivot_tbd}
    r =[]
    for key, value in out_county['County_List'].items():
        if len(value) < 2:
            r.append(key)
    for i in r:
        del out_county['County_List'][i]
#### EXTERNAL FILES FOR REFERNCE NEEDED ####
    df_place = pd.read_csv('place_ref.csv', dtype=str)
    df_place = distr.merge(df_place, left_on='GEOID20', right_on='GEOID20_0KM', suffixes=('_d', '') )
    df_place = df_place.drop(columns=['GEOID20_d','GEOID20_0KM'])
    district_list = list(set(df_place['GEOID20'].values.tolist()))
    pivot_tbd = {}
    for i in district_list:
        pivot_tbd[i] = []
    for idx, row in df_place.iterrows():
        pivot_tbd[row['GEOID20']].append(row[disid])
        pivot_tbd[row['GEOID20']] = list(set(pivot_tbd[row['GEOID20']]))
    r = []
    for key, value in pivot_tbd.items():
        if len(value) < 2:
            r.append(pivot_tbd[key])
    Segement_Count =  0
    Split_Count = 0
    for key,value in pivot_tbd.items():
        if len(value) > 1:
            Segement_Count += len(value)
            Split_Count += 1
    out_place = {'Segement_Count':Segement_Count, 'Split_Count':Split_Count, 'Place_List': pivot_tbd}
    r =[]
    for key, value in out_place['Place_List'].items():
        if len(value) < 2:
            r.append(key)
    for i in r:
        del out_place['Place_List'][i]
    df_mil = pd.read_csv('mil_ref.csv', dtype=str)
    df_mil = distr.merge(df_mil, left_on='GEOID20', right_on='GEOID20_0KM', suffixes=('', '') )
    df_mil = df_mil.drop(columns=['GEOID20','GEOID20_0KM'])
    district_list = list(set(df_mil['AREAID'].values.tolist()))
    pivot_tbd = {}
    for i in district_list:
        pivot_tbd[i] = []
    for idx, row in df_mil.iterrows():
        pivot_tbd[row['AREAID']].append(row[disid])
        pivot_tbd[row['AREAID']] = list(set(pivot_tbd[row['AREAID']]))
    r = []
    for key, value in pivot_tbd.items():
        if len(value) < 2:
            r.append(pivot_tbd[key])
    Segement_Count =  0
    Split_Count = 0
    for key,value in pivot_tbd.items():
        if len(value) > 1:
            Segement_Count += len(value)
            Split_Count += 1
    out_mil = {'Segement_Count':Segement_Count, 'Split_Count':Split_Count, 'Mil_List': pivot_tbd}
    r = []
    for key, value in out_mil['Mil_List'].items():
        if len(value) < 2:
            r.append(key)
    for i in r:
        del out_mil['Mil_List'][i]
    df_vtd = pd.read_csv('vtd_ref.csv', dtype=str)
    df_vtd = distr.merge(df_vtd, left_on='GEOID20', right_on='GEOID20', suffixes=('_d', '') )
    df_vtd = df_vtd.drop(columns=['GEOID20'])
    district_list = list(set(df_vtd['VTD_GEOID20'].values.tolist()))
    pivot_tbd = {}
    for i in district_list:
        pivot_tbd[i] = []
    for idx, row in df_vtd.iterrows():
        pivot_tbd[row['VTD_GEOID20']].append(row[disid])
        pivot_tbd[row['VTD_GEOID20']] = list(set(pivot_tbd[row['VTD_GEOID20']]))
    r = []
    for key, value in pivot_tbd.items():
        if len(value) < 2:
            r.append(pivot_tbd[key])
    Segement_Count =  0
    Split_Count = 0
    for key,value in pivot_tbd.items():
        if len(value) > 1:
            Segement_Count += len(value)
            Split_Count += 1
    out_vtd = {'Segement_Count':Segement_Count, 'Split_Count':Split_Count, 'Vtd_List': pivot_tbd}
    r = []
    for key, value in out_vtd['Vtd_List'].items():
        if len(value) < 2:
            r.append(key)
    for i in r:
        del out_vtd['Vtd_List'][i]
    out = {'vtd' : out_vtd,
        'county' : out_county,
        'place' : out_place,
        'mil' : out_mil}
    ## OUTPUT
    with open('geo_splits.json', 'w') as f:
        json.dump(out, f)
    return out