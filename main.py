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
import datetime as dt

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

OVERWRITE_LOCAL = config['main']['overwrite_local']

# Set API parameters
azure_connection_str = config['api-info']['azure']['connection-str']
azure_container_name = config['api-info']['azure']['container-name']
census_api_key = config['api-info']['census']['key']

# Create Azure Storage and Census API Clients 
c = Census(year=YEARS[-1], key=census_api_key)
azure_manager = utils.AzureBlobStorageManager(connection_str=azure_connection_str, 
                                        container_name=azure_container_name,
                                        download_dir=DOWNLOAD_DIR)
## Obtain census data

# First check if downloading raw data is necessary 
proceed_download = False 
os.makedirs(os.path.join(DOWNLOAD_DIR, 'raw'), exist_ok=True) 
raw_output_path = os.path.join(DOWNLOAD_DIR, f"{'-'.join(CENSUS_VARS)}_{len(STATES)}-States_{'-'.join(YEARS)}.csv")
if os.path.isfile(raw_output_path):
    
    if OVERWRITE_LOCAL: 
        # First check if all the states in the existing output file match those currently specified in the config.yaml file  
        df = pd.read_csv(raw_output_path)
        if (df.shape[0] == 0) or any(col not in df.columns for col in ('state_name', 'year')): 
            logger.warning(f'Output file of same name exists already ({raw_output_path}), but data is missing (file empty or columns missing:\n{utils.df_to_print(df)}). Proceeding with download.')
            proceed_download = True  
        else: 
            df_states = df['state_name'].unique()
            df_years = df['year'].unique()

            if all(var in df.columns for var in CENSUS_VARS) \
                and all(state in df_states for state in STATES)\
                and all(year in df_states for year in YEARS): 
                    logger.info(f'Specified census variables, years, and states already downloaded in {raw_output_path}. Skipping download.')
            else: 
                logger.info(f'Output file of same name exists already ({raw_output_path}), but contains different states/years/census variables from what\'s specified. Proceeding with download.')
                proceed_download = True  
            
            # clear memory after checking file
            del df_states
            del df_years
            del df
    else: 
        logger.info(f"{raw_output_path} exists already, but `config['main']['overwrite_local']`== True. Proceeding with download.")
        proceed_download = True
     
if proceed_download == True: 
    logger.info(f'Downloading {CENSUS_VARS} from {YEARS[0]} to {YEARS[-1]}: {raw_output_path}')

    dataframes = []
    failed_downloads = []
    max_retries = 2

    for state in STATES: 
        n = 0 
        retries = 0    
        while (n < len(YEARS)):  
        
            mhhi_data_year = None
            year = YEARS[n]
    
            try: 
            
                start_time = dt.datetime.utcnow()
                
                mhhi_data_year = c.acs5.state_county_tract(fields = ['NAME'] + CENSUS_VARS,
                                                    state_fips = state['fips'], 
                                                    county_fips = "*",
                                                    tract="*",
                                                    year=year)
                df = pd.DataFrame(mhhi_data_year)
                df['year'] = year
                df['state_fips'] = state['fips']
                df['state_name'] = state['name']
                df['state_usps'] = state['usps']
            
                dataframes.append(df)
                n += 1 
                retries = 0

                end_time = dt.datetime.utcnow()

                elapsed_time = np.round((end_time - start_time).seconds, 2)

                logger.info(f"Downloaded ({state['USPS']}, {str(year)}, {elapsed_time}s, {df.shape[0]} rows, {df.memory_usage(deep=True).sum() / (1024 ** 2)}MB)")

            except Exception as e:
                retries += 1
                logger.warning(f"({state['USPS']}, {str(year)}): {str(e)} (Retrying {retries}/{max_retries})")
        
            if (mhhi_data_year is None) and (retries == max_retries):
                logger.warning(f"Failed download {retries}/{max_retries} -- skipping to next year")
                n += 1 
                retries = 0 
                failed_downloads.append({'state_name':state['name'], 'year':state['year'], 'vars':CENSUS_VARS}) 

    # Log successes/failures
    df = pd.concat(dataframes)
    logger.info(f"Total Download: {df.shape[0]} rows, {df.memory_usage(deep=True).sum() / (1024 ** 2)}MB")
    if len(failed_downloads) > 0: 
        failed_downloads_print = "\n".join(failed_downloads[:min(len(failed_downloads, 10))])
        logger.warning(f'{len(failed_downloads)} failed downloads:\n{failed_downloads_print}')

    # Store raw census download 
    df.to_csv(raw_output_path)

### Transform raw census data
logger.info(f'Raw Census data in {raw_output_path}:\n{utils.df_to_print(df)}\n{df.shape}')

## Long Dataframe 
# Split/edit location columns:  
df[['tract_dec', 'county_name', 'STATENAME']] = df['NAME'].str.split(', ', expand=True)
# Check that state names line up 
state_errors = df[df['STATENAME'] != df['state_name']]
if state_errors.shape[0] > 0: 
    # Kills script -- inspect the raw file in `output_path` 
    logger.exception(f'States do not match expected values in {state_errors.shape[0]} rows:\n {utils.df_to_print(state_errors, rows=min(state_errors.shape[0], 20))}')
df['tract_dec'] = df['tract_dec'].str.lstrip('Census Tract ').str.strip()
df['county_name'] = df['county_name'].str.rstrip('County')
df.rename({'county':'county_fips'}, axis=1, inplace=True)

# Dropping/reordering columns 
df = df[['state_fips', 'state_name', 'county_fips', 'county_name', 'tract', 'tract_dec', 'year'] + CENSUS_VARS]

# Handle NaN values:  
# Negative values are sometimes used to signal missing data -- replacing these with NaNs
rows_w_negative = df[df[CENSUS_VARS].apply(lambda row: any(val < 0 for val in row.values), axis=1)]
n_rows_w_negative = rows_w_negative.shape[0]
if n_rows_w_negative > 0: 
    logger.info(f'{n_rows_w_negative} rows have negative placeholder values for census variables:\n {utils.df_to_print(rows_w_negative.value_counts().reset_index())}')
    logger.info(f'Replacing these values with NaN.')
    df[CENSUS_VARS] = df[CENSUS_VARS].applymap(lambda x: np.nan if x < 0 else x)
nan_counts = df.isna().sum()
logger.info(f'NaN counts:\n {utils.df_to_print(nan_counts)}')

# Handle Duplicates
dups = df[df.duplicated()]
if dups.shape[0] > 0: 
    logger.info(f'{dups.shape[0]} duplicate rows (dropping)')
    df = df[~df.index.isin(dups.index)]

logger.info(f'Transformed Census data (Long):\n{utils.df_to_print(df)}\n{df.shape}')

## Widen dataframe
df = pd.pivot(data=df, 
            index=df.filter(regex='state|tract|county').columns,
            columns=['year'], 
            values=CENSUS_VARS)
# Fix column names 
df.columns = ['-'.join((cvar, str(year))) for (cvar, year) in df.columns]

logger.info(f'Widened data: \n{utils.df_to_print(df)}\n{df.shape}')

## Collapse each census variable column into nested JSON format
for var in CENSUS_VARS: 
    var_year_columns = df.filter(regex=var).columns
    df[var] = df[var_year_columns] \
        .apply(lambda row: {str(col.split('-')[1]):np.round(row_value, 2) for col, row_value in row.items()}, axis=1)
    df.drop(var_year_columns, axis=1, inplace=True)

logger.info(f'Collapsed data: \n{utils.df_to_print(df)}\n{df.shape}')

## Apply the years crosswalk

# Obtain crosswalk from Azure Container or re-create from source data
from tract_crosswalk import get_tract_crosswalks 
map_10_to_20 = get_tract_crosswalks()[0] # We want the crosswalk from 2010 to 2020. TO-DO: set the direction of the conversion as a parameter in config.yaml, pass into get_tract_crosswalks
df_map_10_to_20 = pd.DataFrame(map_10_to_20)

## Join with census data 
# Create column for joining 
df['GEOID_TRACT_10'] = [''.join([idx[0], idx[2], str(idx[4])]) for idx in df.index]
joined = df.merge(df_map_10_to_20, how='left', left_on=['GEOID_TRACT_10'], right_index=True)

# Engineer historical data for 2020 tracts 
for var in CENSUS_VARS: 
    utils.apply_crosswalk(joined)






# Apply crosswalk to data 

# 

# Log successes/failures 
## size of download (in rows and bytes) 
## download

## Transform data 