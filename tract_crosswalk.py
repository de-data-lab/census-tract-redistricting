import pandas as pd
import requests
import geopandas as gpd 
import pygris
import pygris.utils
import json
import sys
import os
import us 
import re
from logger import logger
from utils import AzureBlobStorageManager, load_state_list, validate_config, read_json_rows
import yaml
import datetime as dt
import shapely
import numpy as np
import sys, getopt
import yaml
import utils


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

# Create azure client 
azure_manager = AzureBlobStorageManager(connection_str=config['api-info']['azure']['connection-str'], 
                                        container_name=config['api-info']['azure']['container-name'], 
                                        download_dir=DATA_DIR)

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


def _get_tract_geoms_state(year:int, state:str) -> gpd.GeoDataFrame:
    """Loads tract geometries from pygris for all tracts in a given state and year."""

    tract_geoms = pygris.tracts(year=year, state=state)

    default_geoid_col = 'GEOID' if year == 2020 else 'GEOID10'
    year_abbrev = str(year)[-2:]

    tract_geoms.rename({default_geoid_col:f'GEOID_TRACT_{year_abbrev}'}, axis=1, inplace=True) 

    # tract_geoms.rename({default_geoid_col:f'GEOID_TRACT_{year_abbrev}', 
    #                     'geometry':f'geometry_{year_abbrev}'}, axis=1, inplace=True) 
            # renaming geometry column is for calculating overlaps between 2010 and 2020, geometry_10 and geometry_20 
    
    tract_geoms[f'GEOID_TRACT_{year_abbrev}'] = tract_geoms[f'GEOID_TRACT_{year_abbrev}'].astype(str)

    tract_geoms = tract_geoms[[f'GEOID_TRACT_{year_abbrev}', 'geometry']] \
        .set_index(f'GEOID_TRACT_{year_abbrev}') 
    
    # Cast to geodataframe
    tract_geoms = gpd.GeoDataFrame(tract_geoms)

    return tract_geoms

def get_all_tract_geoms_year(year:int, erase_water=False, simplify_tolerance=None, buffer_size=.5, update_azure=False) -> gpd.GeoDataFrame: 
    """Download or load cached pygris geometries for all states + DC and Puerto Rico.
    For purposes of calculating area overlaps in _calc_percent_overlaps(), none of the modification parameters will be applied.
    
    Args: 

    erase_water (bool): Whether to remove water from the geometries
    
    simplify_tolerance (int): Tolerance level to simplify geometries by. Ignored if 'None'.

    buffer_size (float): Amount to buffer geometries by (used to prevent boundary issues when removing water).
        Ignored if erase_water == False.

    update_azure (bool): Whether to update     

    Returns: 

    (gpd.GeoDataFrame): GeoDataFrame of all tract geometries for all states for a given year. 
    
    """

    ## download geometries (TO-DO: change _get_tract_geoms_state() to return a list of dicts and then convert finished to dataframe after loop. 
    # Compare speed vs appending dataframes and concatenating like here)
 
    # Construct the path for the geometries GeoJSON
    ew = 'erase-water' if erase_water else ''
    bs =  f'buffer-{buffer_size}' if erase_water else ''
    st = f'tol-{simplify_tolerance}' if simplify_tolerance is not None else ''

    end_str = 'raw' if all(x == '' for x in (ew, st, bs)) else '_'.join((ew, bs, st))
  
    tract_geom_path = os.path.join(DATA_DIR, f'pygris_{year}_tract-geoms_allStates+DC+PR{end_str}.json')
    
    logger.info(f'Using geometry path: {tract_geom_path}')

    # Check that geoms exist locally 
    if os.path.isfile(tract_geom_path): 
        logger.info(f'Loading cached geoms ({tract_geom_path})')
        geoms = gpd.read_file(tract_geom_path)
        geoms.set_index('GEOID_TRACT_20', inplace=True)
    else: 
        # Check azure to download complete geoms first 
        if azure_manager.has_blob(tract_geom_path):
            logger.info(f'Downloading complete geoms from Azure {os.path.basename(tract_geom_path)}')
            start_time = dt.datetime.utcnow()
            azure_manager.download_blob(blob_name=os.path.basename(tract_geom_path), 
                                        download_path=tract_geom_path)
            end_time = dt.datetime.utcnow()
            elapsed_time = np.round((end_time-start_time).seconds, 3)
            logger.info(f'Download complete ({elapsed_time}s)')

        else: 
            logger.info(f'Downloading {year} tract geoms from pygris')

            state_list = utils.load_state_list(states=['All'], 
                            include_dc=True, 
                            include_pr=True) 
            
            geoms = []
            for n,state in enumerate(state_list):
                
                # Download the tract geoms for the state and year  
                start_time = dt.datetime.now()
                geoms_state = _get_tract_geoms_state(state=state['fips'], year=year)
                end_time = dt.datetime.now()
                elapsed_time = np.round((end_time - start_time).seconds, 3)
                if n % 5 == 0: 
                    logger.info(f'Downloaded {year} tract geoms for {state["fips"]}-{state["usps"]} ({elapsed_time}s)')

                if erase_water: 
                    try: 
                        # Apply buffer to geometries to avoid errors with removing water
                        geoms_state['geometry'] = geoms_state['geometry'].buffer(buffer_size)
                        # Remove water from the geometries
                        start_time = dt.datetime.now()
                        geoms_state = pygris.utils.erase_water(geoms_state)
                        end_time = dt.datetime.now()
                        elapsed_time = np.round((end_time - start_time).seconds, 3)
                        if n % 5 == 0: 
                            logger.info(f'Removed water ({elapsed_time}s)')
                    except Exception as e: 
                        logger.exception(f'Failed to remove water for {state["fips"]}-{state["usps"]})')
                        raise Exception(e)

                if simplify_tolerance is not None: 
                    try: 
                        # Simplify the geometries
                        start_time = dt.datetime.now()
                        geoms_state['geometry'] = geoms_state['geometry'].simplify(tolerance=simplify_tolerance)
                        end_time = dt.datetime.now()
                        elapsed_time = np.round((end_time - start_time).seconds, 3)
                        if n % 5 == 0: 
                            logger.info(f'Simplified geometries (tolerance={simplify_tolerance}) ({elapsed_time}s)')
                    except Exception as e: 
                        logger.exception(f'Failed to simplify geometries for {state["fips"]}-{state["usps"]}')
                        raise Exception(e)
                
                geoms.append(geoms_state)

            geoms = pd.concat(geoms)

            # Saving geometries locally
            logger.info(f'Saving {year} tract geoms ({tract_geom_path})')
            geoms.to_file(tract_geom_path)

        # Upload to azure if filename not found there
        if not azure_manager.has_blob(tract_geom_path) and update_azure: 
            logger.info(f'Uploading geoms to Azure ({os.path.basename(tract_geom_path)})')        
            azure_manager.upload_blob(tract_geom_path)

    return geoms

def _calc_pct_overlap(parent_geom, child_geom): 
    """Calculate percentage overlap between two geometries."""

    percent_overlap = (parent_geom.intersection(child_geom).area / child_geom.area)
    return percent_overlap


def create_year_conversion_map(df:pd.DataFrame, start_year:int, end_year:int, dec_to_round:int) -> list:
    """Create json row object mapping tracts from one year to another with percentage overlap"""

    logger.info(f"Creating conversion map from {start_year} to {end_year} tracts.")

    start_year_abrev, end_year_abrev = str(start_year)[-2:], str(end_year)[-2:]
    start_geoid, end_geoid = f"GEOID_TRACT_{start_year_abrev}", f"GEOID_TRACT_{end_year_abrev}"
    pct_overlap_col = f'pct_overlap_{start_year_abrev}-to-{end_year_abrev}'

    # We can't use pd.pivot since the values for the widened columns aren't in the base df and we must calculate how many of them we need 
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
        config = yaml.full_load(file)

    azure_manager = AzureBlobStorageManager(connection_str=config['api-info']['azure']['connection-str'], 
                                            container_name=config['api-info']['azure']['container-name'], 
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

    ## load/download geometries
    geoms_10 = get_all_tract_geoms_year(year=2010)
    geoms_20 = get_all_tract_geoms_year(year=2020)

    # Join to the conversion table 
    conversion_table_w_geoms = conversion_table\
        .merge(geoms_10, how='inner', on='GEOID_TRACT_10')\
        .merge(geoms_20, how='inner', on='GEOID_TRACT_20')\
        .rename({'geometry_x':'geometry_10', 'geometry_y':'geometry_20'}, axis=1)

    # Calculate percent overlap bidirectionally 
    conversion_table_w_geoms[['pct_overlap_10-to-20', 'pct_overlap_20-to-10']] = conversion_table\
        .apply(lambda row: pd.Series([_calc_pct_overlap(row['geometry_10'], row['geometry_20']), 
                                        _calc_pct_overlap(row['geometry_20'], row['geometry_10'])]), axis=1)
        
    # Create maps from one year to the other
    map_10_to_20 = create_year_conversion_map(conversion_table_w_geoms, start_year=2010, end_year=2020, dec_to_round=OVERLAP_PRECISION)
    map_20_to_10 = create_year_conversion_map(conversion_table_w_geoms, start_year=2020, end_year=2010, dec_to_round=OVERLAP_PRECISION)
                                              
    ## Saving crosswalks: 
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
    """Load tract crosswalk from 2010 to 2020.
    
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