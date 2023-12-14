import pandas as pd
import requests
import geopandas as gpd 
import pygris
import json
import sys
import os
import us 
from logger import logger
from utils import AzureBlobStorageManager, load_state_list, validate_config, read_json_rows
import yaml
import datetime as dt
import shapely
import numpy as np
import sys, getopt
import yaml

## Load and validate parameters from config file 
module_dir = os.path.dirname(os.path.abspath(__file__)) 
with open(os.path.join(module_dir, 'config.yaml'), 'r') as file: 
    config = yaml.full_load(file)
validate_config(config)

# Set parameters from config 
DATA_DIR = config['data_dir']
OVERLAP_PRECISION = config['tract_crosswalk']['overlap_precision']
OVERWRITE_AZURE = config['tract_crosswalk']['overwrite_azure']
OVERWRITE_LOCAL = config['tract_crosswalk']['overwrite_local']

# Initialize paths for output
os.makedirs(os.path.join(DATA_DIR, 'raw'), exist_ok=True)
raw_conversion_path = os.path.join(DATA_DIR, 'raw', 'tract_conversion_table_2010-2010_raw.csv') # Path to save raw conversion table from census.gov (doesn't have percentage overlaps)
crosswalk_paths = [os.path.join(DATA_DIR, fp) for fp in ('convert-ctracts_pct-area_2010-to-2020.json', 
                                                      'convert-ctracts_pct-area_2020-to-2010.json')]

## --------------------------------------------------------------------------------------- ##


def _scrape_raw_conversion_table(file_path:str) -> None:
    """Get conversion table of all census tracts in all states from 2010 to 2020 Census. First checks if there is a locally cached file, then scrapes and saves file if it doesn't exist."""

    ## Check if cached file exists already  
    if os.path.isfile(file_path): 
        logger.info(f'{file_path} already exists. Skipping scrape from USCB.')

    else:
        
        logger.info(f'{file_path} does not exist. Scraping from remote and writing to path.')
        
        state_list = load_state_list()

        with open(file_path, 'w') as out_file: 
            headers = 'STATENAME' + '|' + 'OID_TRACT_20|GEOID_TRACT_20|NAMELSAD_TRACT_20|AREALAND_TRACT_20|AREAWATER_TRACT_20|MTFCC_TRACT_20|FUNCSTAT_TRACT_20|OID_TRACT_10|GEOID_TRACT_10|NAMELSAD_TRACT_10|AREALAND_TRACT_10|AREAWATER_TRACT_10|MTFCC_TRACT_10|FUNCSTAT_TRACT_10|AREALAND_PART|AREAWATER_PART'
            out_file.write(headers + '\n') 
            for state_dict in state_list: 

                # Scrape file for specific state -- can't use national aggregate file because it doesn't include a state column 
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


def get_tract_crosswalks() -> None:
    """Download the crosswalks between 2010 and 2020 census tracts with percentage overlaps"""

    # Create Azure Client
    with open('config.yaml', 'r') as file: 
        api_info = yaml.full_load(file)

    azure_manager = AzureBlobStorageManager(connection_str=api_info['api-info']['azure']['connection-str'], 
                                            container_name=api_info['api-info']['azure']['container-name'], 
                                            download_dir=DATA_DIR)
    
    ## --- Determine whether to re-run script or use existing copies of the crosswalks --- ## 
    logger.info(f"(OVERWRITE_AZURE={config['tract_crosswalk']['overwrite_azure']}, OVERWRITE_LOCAL={config['tract_crosswalk']['overwrite_local']})")
    check_files_available = [{'file':os.path.basename(fp), 'local':os.path.isfile(fp), 'azure':azure_manager.has_blob(fp)} for fp in crosswalk_paths]
    logger.info(f"Checking if crosswalks exist:\n{check_files_available[0]}\n{check_files_available[1]}")

    # Checking local directory 
    if any(not (d['local']) for d in check_files_available): 
        logger.info(f"Commencing download (crosswalks missing in {DATA_DIR}/).")
    elif OVERWRITE_LOCAL:  
        logger.info(f"Overwriting local crosswalks.")
    else: 
        logger.info(f"Local crosswalks exist and OVERWRITE_LOCAL={OVERWRITE_LOCAL}. Aborting script.")
        return 

    # Checking Azure
    if all(d['azure'] for d in check_files_available): 
        if OVERWRITE_AZURE: 
            logger.info(f'Re-creating crosswalks and overwriting files in Azure.')
        else: 
            logger.info(f"Downloading crosswalks from Azure container:")
            start_time = dt.datetime.utcnow()
            for fp in crosswalk_paths: 
                azure_manager.download_blob(os.path.basename(fp))
            end_time = dt.datetime.utcnow()
            logger.info(f'Downloads complete ({(end_time - start_time).seconds})s')
            return     
    ## ----------------------------------------------------------------------------------- ##  
    # If above blocks were passed, proceed with script 
     
    ## scrape / load conversion table   
    _scrape_raw_conversion_table(raw_conversion_path)
    conversion_table = pd.read_csv(raw_conversion_path, sep='|', dtype={'GEOID_TRACT_10':str, 'GEOID_TRACT_20':str})
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
    for fp, conversion_map in zip(crosswalk_paths, (map_10_to_20, map_20_to_10)): 
        # Locally 
        with open (fp, 'w') as file: 
            for row in conversion_map: 
                json.dump(row, file)
                file.write('\n')
        # In Azure
        azure_manager.upload_blob(fp, overwrite=OVERWRITE_AZURE)
         
    return 

def load_crosswalk_2010_2020(reverse=False) -> list: 
    """Load tract crosswalk from 2010 to 2020. Uses the absolute path of 
    
    Args:

    reverse (bool): If True, loads the crosswalk from 2020 tracts to 2010 tracts. Default=False.

    Returns: 

    crosswalk (list): Tract area crosswalk in JSON row format 
    
    """

    fp = crosswalk_paths[0] if not reverse else crosswalk_paths[1]
    if os.path.isfile(fp): 
        return read_json_rows(fp)
    else: 
        raise Exception(f"Crosswalk {fp} does not exist -- run get_tract_crosswalks()")

if __name__ == "__main__":
    get_tract_crosswalks()