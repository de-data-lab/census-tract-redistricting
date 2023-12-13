import json
import yaml
import pandas as pd 
import geopandas
import pygris 
from census import Census 
import utils 
from logger import logger
import os
import sys
import numpy as np
import geopandas as gpd

## Load and validate parameters from config file 
config_file = 'config.yaml'
with open(config_file, 'r') as file: 
    config = yaml.full_load(file)

validation_error_dict = utils.validate_config(config)
for param, error in validation_error_dict.items(): 
    logger.error(f'{config_file} -- {param}: {error}')
if len(validation_error_dict) > 0: 
    raise Exception(f"Revise parameters in {config_file}")

# Set census parameters 
CENSUS_VARS = config['census_vars']
YEARS = range(config['start_year'], config['end_year'])
STATES = utils.load_state_list(config['states'])
DOWNLOAD_DIR = config['data_dir']

# Set API parameters
azure_connection_str = config['api-info']['azure']['connection-str']
azure_container_name = config['api-info']['azure']['container-name']
census_api_key = config['api-info']['census']['key']

# Create Azure Storage and Census API Clients 
c = Census(year=YEARS[-1], key=census_api_key)
azure_manager = utils.AzureBlobStorageManager(connection_str=azure_connection_str, 
                                        container_name=azure_container_name,
                                        download_dir=DOWNLOAD_DIR)

## Obtain crosswalks from Azure Container or re-create from source data
from tract_crosswalk import main 
main()

## Obtain census data 
logger.info(f'Downloading {CENSUS_VARS} from {YEARS[0]} to {YEARS[-1]}')

dataframes = []
failed_downloads = []
max_retries = 2

for state in STATES: 
    n = 0 
    retries = 0    
    while (n < len(YEARS)):  

        logger.info(f"Downloading ({state['USPS']}, {str(year)})")
        mhhi_data_year = None
        year = YEARS[n]
 
        try: 
           
            mhhi_data_year = c.acs5.state_county_tract(fields = ['NAME'] + CENSUS_VARS,
                                                state_fips = state['fips'], 
                                                county_fips = "*",
                                                tract="*",
                                                year=year)
            mhhi_df = pd.DataFrame(mhhi_data_year)
            mhhi_df['year'] = year
            mhhi_df['state_fips'] = state['fips']
            mhhi_df['state_name'] = state['name']
            mhhi_df['state_usps'] = state['usps']
        
            dataframes.append(mhhi_df)
            n += 1 
            retries = 0

        except Exception as e:
            retries += 1
            logger.warning(f"({state['USPS']}, {str(year)}): {str(e)} (Retrying {retries}/{max_retries})")
    
        if (mhhi_data_year is None) and (retries == max_retries):
            logger.warning(f"Failed download {retries}/{max_retries} -- skipping to next year")
            n += 1 
            retries = 0 
            failed_downloads.append({'state_name':state['name'], 'year':state['year'], 'vars':CENSUS_VARS}) 
        
# Log successes/failures 
## size of download (in rows and bytes) 
## download
logger.info(f"{}")


## Transform data 