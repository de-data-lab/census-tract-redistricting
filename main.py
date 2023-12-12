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

# Set API parameters
azure_connection_str = config['api-info']['azure']['connection-str']
azure_container_name = config['api-info']['azure']['container-name']
census_api_key = config['api-info']['census']['key']

## Obtain crosswalks from Azure Container or re-create from source data
from get_tract_crosswalk import main 
main()


## Obtain census data 





## Transform data 