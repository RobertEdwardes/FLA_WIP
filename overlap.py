import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup, SoupStrainer
import zipfile
import wget
import os
import requests
from datetime import datetime


def getData(State=None):
    dir_list = os.listdir()
    state_ids = pd.read_csv('us-state-ansi-fips.csv',skipinitialspace=True, dtype=str)
    st = state_ids[state_ids['st']==State]
    st = st['stname'].iloc[0]
    if '2020_PLSummaryFile_FieldNames.xlsx' not in dir_list:
        headers='https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020_PLSummaryFile_FieldNames.xlsx'
        wget.download(headers)
    url = f'https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/{st}/'
    r = requests.get(url)
    res = r.content
    for link in BeautifulSoup(res, parse_only=SoupStrainer('a')):
        if 'zip' in link.contents[0]:
            f = link.contents[0]
    if f not in dir_list:
        file = f'{url}{f}'
        wget.download(file)
    if f'PL94_blocks_{State}.csv' not in dir_list:
        df_header1 = pd.read_excel('2020_PLSummaryFile_FieldNames.xlsx', sheet_name='2020 P.L. Segment 1 Definitions').dropna(axis=0, how='all').reset_index(drop=True)
        df_header2 = pd.read_excel('2020_PLSummaryFile_FieldNames.xlsx', sheet_name='2020 P.L. Segment 2 Definitions').dropna(axis=0, how='all').reset_index(drop=True)
        df_header3 = pd.read_excel('2020_PLSummaryFile_FieldNames.xlsx', sheet_name='2020 P.L. Segment 3 Definitions').dropna(axis=0, how='all').reset_index(drop=True)
        df_headergeo = pd.read_excel('2020_PLSummaryFile_FieldNames.xlsx', sheet_name='2020 P.L. Geoheader Definitions').dropna(axis=0, how='all').reset_index(drop=True)
        header_replace_1 = {i :None for i in range(0,len(df_header1.index)) }
        header_replace_2 = {i :None for i in range(0,len(df_header2.index)) }
        header_replace_3 = {i :None for i in range(0,len(df_header3.index)) }
        header_replace_geo = {i :None for i in range(0,len(df_headergeo.index)) }
        array = [[df_header1,header_replace_1, '1'],[df_header2,header_replace_2,'2'],[df_header3,header_replace_3,'3'],[df_headergeo,header_replace_geo,'o']]
        for i in array:
            json = i[1]
            header = i[0]
            for key in json.keys():
                json[key] = header.iloc[key][1]
        archive = zipfile.ZipFile(f, 'r')
        csv = []
        for i in archive.infolist():
            temp = archive.open(i)
            fileName = temp.name.split('.')[0]
            fileNum = fileName[-5:][0]
            df = pd.read_csv(temp, sep="|", header=None, low_memory=False ,encoding = "ISO-8859-1")
            for j in array:
                if fileNum == j[2] :
                    df = df.rename(columns=j[1])
            df.to_csv(f'{fileName}.csv', index=False)
            csv.append(fileName)
        join_on = ['STUSAB','LOGRECNO']
        df_out = None
        for i in csv:
            if df_out is None:
                df_out = pd.read_csv(f'{i}.csv', low_memory=False, dtype={'FILEID':'str','STUSAB':'str','CHARITER':'str','CIFSN':'str','LOGRECNO':'str'})
                continue
            else:
                df = pd.read_csv(f'{i}.csv', low_memory=False, dtype={'FILEID':'str','STUSAB':'str','CHARITER':'str','CIFSN':'str','LOGRECNO':'str'})
                df_out = df_out.merge(df, on=join_on, suffixes=('', '_y'))
                delt = []
                for k in df_out.columns:
                    if '_y' in k:

                        delt.append(k)
                df_out = df_out.drop(columns=delt)
        r = [i for i in df_out.columns if i[0:2] != 'P0' or i != 'GEOID20']
        df_out = df_out[df['SUMLEV'] == 750]
        
        keep = ['POP100',
        'CD116',
        'SLDU18',
        'SLDL18',
        'GEOCODE']
        r = [i for i in df_out.columns if i not in keep]
        df_out = df_out.drop(columns=r)
        df_out.to_csv(f'PL94_blocks_{State}.csv', index=False)
        for i in csv:
            os.remove(f'{i}.csv')
    else:
        df_out = pd.read_csv(f'PL94_blocks_{State}.csv', dtype={'GEOCODE':str,'CD116':str,'SLDU18':str,'SLDL18':str})
    return df_out

#####  PROCESS Off PL94 File  ######
def Overlap_old_new(new_districts, geoid='GEOID', district='District', leg=None):

#####  CHECKS  ######
    if leg not in ['CD116','SLDU18','SLDL18'] or leg is None:
        raise Exception('Please Choose Legislative level Default CD')
    if not isinstance(new_districts ,pd.DataFrame):
        raise Exception('Please Supply a path to a CSV or DataFrame for new districts')
    if district not in new_districts.columns:
        raise Exception('District column not given or is missing from a DataFrame')
    if geoid not in new_districts.columns:
        raise Exception('GEOID column not given or is missing from a DataFrame')
    fips = new_districts[geoid].iloc[0]
    old_org = getData(State=str(fips[0:2]))
    old_org = old_org.rename(columns={'GEOCODE':geoid,leg:district})
    data = old_org[[geoid,'POP100']]
    old_districts = old_org[[geoid,district]]
#####  PROCESS  ######

    groupby_pop = district
    old = data.merge(old_districts, on=geoid)
    new = data.merge(new_districts, on=geoid)
    new = new.drop_duplicates()
    old = old.drop_duplicates()
    cross = old[[geoid,district]].merge(new,
                                        on=geoid,
                                        suffixes=('_old', '_new'))
    cross = cross.drop_duplicates()
    new = new.drop(columns='GEOID20')
    old = old.drop(columns='GEOID20')
    cross = cross.drop(columns='GEOID20')
    old = old.groupby([groupby_pop], as_index=False).sum()
    new = new.groupby([groupby_pop], as_index=False).sum()
    new_group = [district+'_old',district+'_new']
    cross_old, cross_new = [district+'_old',district+'_new']
    groupby_cross = district
    cross = cross.groupby([f'{groupby_cross}_old',f'{groupby_cross}_new'], as_index=False ).sum()
    new = cross.merge(new, left_on=cross_new, right_on=district,suffixes=('_new', '_cross'))
    old = cross.merge(old, left_on=cross_old, right_on=district,suffixes=('_old', '_cross'))
    out = new.merge(old, on=[f'{district}_new',f'{district}_old'] , suffixes=('', '_d'))
    r = [district]
    for col in out.columns:
        if '_d' in col:
            r.append(col)
    out = out.drop(columns=r)
    ## OUTPUT
    out.to_csv(f'out_population_overlap.csv',index=False)
    return out

#####  PROCESS Off 2 BAF ######
def Overlap_compare(old_districts, new_districts, data, geoid='GEOID', district='District'):

#####  CHECKS  ######
    if district not in old_districts.columns or district not in new_districts.columns:
        raise Exception('District column not given or is missing from a DataFrame')
    if geoid not in old_districts.columns or geoid not in new_districts.columns or geoid not in data.columns:
        raise Exception('GEOID column not given or is missing from a DataFrame')


#####  PROCESS ######

    groupby_pop = district
    old = data.merge(old_districts, on=geoid)
    new = data.merge(new_districts, on=geoid)
    new = new.drop_duplicates()
    old = old.drop_duplicates()
    cross = old[[geoid,district]].merge(new,
                                     on=geoid,
                                     suffixes=('_old', '_new'))
    cross = cross.drop_duplicates()
    new = new.drop(columns=geoid)
    old = old.drop(columns=geoid)
    cross = cross.drop(columns=geoid)
    old = old.groupby([groupby_pop], as_index=False).sum()
    new = new.groupby([groupby_pop], as_index=False).sum()
    new_group = [district+'_old',district+'_new']
    cross_old, cross_new = [district+'_old',district+'_new']
    groupby_cross = district
    cross = cross.groupby([f'{groupby_cross}_old',f'{groupby_cross}_new'], as_index=False ).sum()
    new = cross.merge(new, left_on=cross_new, right_on=district,suffixes=('_new', '_cross'))
    old = cross.merge(old, left_on=cross_old, right_on=district,suffixes=('_old', '_cross'))
    out = new.merge(old, on=[f'{district}_new',f'{district}_old'] , suffixes=('', '_d'))
    r = [district]
    for col in out.columns:
        if '_d' in col:
            r.append(col)
    out = out.drop(columns=r)
    ## OUTPUT
    out.to_csv(f'out_population_overlap.csv',index=False)
    return out