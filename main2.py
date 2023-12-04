import pandas as pd
import requests
import geopandas as gpd 
import pygris
import json
import sys
import os
import us 
from logger import logger
from utils import AzureBlobStorageManager
import yaml
import datetime as dt
import shapely
import numpy as np
import sys, getopt


DATA_DIR = 'data' 
CONVERSION_PATH = os.path.join(DATA_DIR, 'tract_conversion_table_2010-2010_raw.csv') # Path to save raw conversion table from census.gov (doesn't have percentage overlaps)
# GEOMS_PATH = os.path.join(DATA_DIR, 'tract_conversion_table_2010-2010_geometries.json')
OUTPUT_PATHS = [os.path.join(DATA_DIR, fp) for fp in ('convert-ctracts_pct-area_2010-to-2020.json', 
                                                      'convert-ctracts_pct-area_2020-to-2010.json')]
OVERLAP_PRECISION = 3 # how many decimals you want to calculate the overlap with
# ^^ Note that changing this value could cause your local version to deviate from the one in Azure 
# I *could`d* write this so that it changes the filepath by the precision level but I don't think that's necessary  
OVERWRITE_AZURE = True # Change this to overwrite the version in Azure
OVERWRITE_LOCAL = True # Change this to overwrite your local version of the conversion map  


def _load_state_fips(): 
    """Load list of state fips and names from us module"""
    state_fips_map = [{"name":s.name, "fips":s.fips} for s in us.states.STATES] \
        + [{'name':'District of Columbia', 'fips':'11'}] + [{'name':'Puerto Rico', 'fips':'72'}]
    return state_fips_map


def _scrape_conversion_table(file_path:str) -> None:
    """Get conversion table of all census tracts in all states from 2010 to 2020 Census. First checks if there is a locally cached file, then scrapes and writes file if it doesn't exist."""

    ## Check if cached file exists already  
    if os.path.isfile(file_path): 
        logger.info(f'{file_path} already exists. Skipping scrape.')

    else:
        
        logger.info(f'{file_path} does not exist. Scraping from remote and writing to path.')
        
        state_fips_map = _load_state_fips()


        with open(file_path, 'w') as out_file: 
            headers = 'STATENAME' + '|' + 'OID_TRACT_20|GEOID_TRACT_20|NAMELSAD_TRACT_20|AREALAND_TRACT_20|AREAWATER_TRACT_20|MTFCC_TRACT_20|FUNCSTAT_TRACT_20|OID_TRACT_10|GEOID_TRACT_10|NAMELSAD_TRACT_10|AREALAND_TRACT_10|AREAWATER_TRACT_10|MTFCC_TRACT_10|FUNCSTAT_TRACT_10|AREALAND_PART|AREAWATER_PART'
            out_file.write(headers + '\n') 
            for state_dict in state_fips_map: 

                # Scrape file for specific state -- can't use national aggregate because it doesn't include a state column 
                url = 'https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_st' + state_dict['fips'] + '.txt'
                conversion_table = requests.get(url)

                lines = conversion_table.text.splitlines()
                lines[0] = lines[0].replace('ï»¿', '')  # correct typo
                lines.pop(0) # remove headers 

                for line in lines: 
                    # Add state name  
                    line = state_dict['name'] + '|' + line + '\n'
                    out_file.write(line)

def _get_tract_geoms_state(year:int, state:str) -> pd.DataFrame:
    """Loads tract geometries from pygris for all tracts in a given state and year."""

    tract_geoms = pygris.tracts(year=year, state=state)

    default_geoid_col = 'GEOID' if year == 2020 else 'GEOID10'
    year_abbrev = str(year)[-2:]

    tract_geoms.rename({default_geoid_col:f'GEOID_TRACT_{year_abbrev}', 
                        'geometry':f'geometry_{year_abbrev}'}, axis=1, inplace=True)
    
    tract_geoms[f'GEOID_TRACT_{year_abbrev}'] = tract_geoms[f'GEOID_TRACT_{year_abbrev}'].astype(str)

    tract_geoms = tract_geoms[[f'GEOID_TRACT_{year_abbrev}', f'geometry_{year_abbrev}']] \
        .set_index(f'GEOID_TRACT_{year_abbrev}')
    
    return tract_geoms


def _calc_pct_overlap(parent_geom, child_geom): 
    """Calculate percentage overlap between two geometries."""

    percent_overlap = (parent_geom.intersection(child_geom).area / child_geom.area)
    return percent_overlap


def create_year_conversion_map(df:pd.DataFrame, start_year:int, end_year:int, dec_to_round:int) -> list:
    """Create json row object mapping tracts from one year to another with percentage overlap"""


    start_year_abrev, end_year_abrev = str(start_year)[-2:], str(end_year)[-2:]
    start_geoid, end_geoid = f"GEOID_TRACT_{start_year_abrev}", f"GEOID_TRACT_{end_year_abrev}"
    pct_overlap_col = f'pct_overlap_{start_year_abrev}-to-{end_year_abrev}'


    # We can't use pd.pivot since the values for the widened columns aren't in the base df and the number of them needs to be calculate 
    year_conversion_map = df.groupby(['STATENAME', start_geoid])[[end_geoid, pct_overlap_col]] \
        .agg(list)\
        .apply(lambda row: {x[0]:np.round((x[1]), dec_to_round) for x in zip(row[end_geoid], row[pct_overlap_col])}, axis=1)\
        .reset_index()\
        .rename({0:f'{end_geoid}_overlap'}, axis=1) \
        .to_dict(orient='records')

    return year_conversion_map

def main() -> None:
    """Download the crosswalks between 2010 and 2020 census tracts with percentage overlaps"""

    ## Check azure container if files already exist
    with open('api_info.yaml', 'r') as file: 
        api_info = yaml.full_load(file)

    azure_manager = AzureBlobStorageManager(connection_str=api_info['azure']['connection-str'], 
                                            container_name=api_info['azure']['container-name'], 
                                            download_dir=DATA_DIR)
    for fp in OUTPUT_PATHS: 
        if os.path.isfile(fp) and not OVERWRITE_LOCAL: 
            logger.info(f'{os.path.basename(fp)} already exists locally. Skipping scrape/download.')
            return
        elif azure_manager.has_blob(fp) and not OVERWRITE_AZURE:
            logger.info(f'{os.path.basename(fp)} found in Azure container ({azure_manager.container_name}) but not locally. Downloading to {DATA_DIR}/.')
            start_time = dt.datetime.utcnow()
            azure_manager.download_blob(os.path.basename(fp))
            end_time = dt.datetime.utcnow()
            logger.info(f'Download complete ({(end_time - start_time).seconds})s')    
            return 
        
        else:
            logger.info(f'Commencing script.')

    ## scrape / load conversion table 
    _scrape_conversion_table(CONVERSION_PATH)
    conversion_table = pd.read_csv(CONVERSION_PATH, sep='|', dtype={'GEOID_TRACT_10':str, 'GEOID_TRACT_20':str})
    conversion_table = conversion_table[['STATENAME','GEOID_TRACT_10', 'GEOID_TRACT_20']]

    ## download geometries (TO-DO: change _get_tract_geoms_state() to return a list of dicts and then convert finished to dataframe after loop. Compare speed vs appending dataframes and concatenating like here)
    geoms_10, geoms_20 = [], []  
    for state in conversion_table['STATENAME'].unique():
        geoms_10_state, geoms_20_state = _get_tract_geoms_state(state=state, year=2010), _get_tract_geoms_state(state=state, year=2020)
        geoms_10.append(geoms_10_state)
        geoms_20.append(geoms_20_state)
    geoms_10 = pd.concat(geoms_10)
    geoms_20 = pd.concat(geoms_20)
    

    # Join to the conversion table 
    conversion_table = conversion_table\
        .merge(geoms_10, how='left', left_on='GEOID_TRACT_10', right_index=True)\
        .merge(geoms_20, how='left', left_on='GEOID_TRACT_20', right_index=True)

    # Calculate percent overlap bidirectionally 
    conversion_table[['pct_overlap_10-to-20', 'pct_overlap_20-to-10']] = conversion_table\
        .apply(lambda row: pd.Series([_calc_pct_overlap(row['geometry_10'], row['geometry_20']), 
                                        _calc_pct_overlap(row['geometry_20'], row['geometry_10'])]), axis=1)
        
    # Create maps from one year to the other
    map_10_to_20 = create_year_conversion_map(conversion_table, start_year=2010, end_year=2020, dec_to_round=OVERLAP_PRECISION)
    map_20_to_10 = create_year_conversion_map(conversion_table, start_year=2020, end_year=2010, dec_to_round=OVERLAP_PRECISION)

    ## Saving outputs: 
    for fp, conversion_map in zip(OUTPUT_PATHS, (map_10_to_20, map_20_to_10)): 
        # Locally 
        with open (fp, 'w') as file: 
            for row in conversion_map: 
                json.dump(row, file)
                file.write('\n')
        # In Azure
        azure_manager.upload_blob(fp, overwrite=OVERWRITE_AZURE)

    ## TO-DO: 
    # Decide on final data model
    # Run script for all states and tracts  
    # Write motebook demonstrating how to use the crosswalk map
    # Change the README.md

if __name__ == "__main__":
    main()