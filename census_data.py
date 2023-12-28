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

from tract_crosswalk import get_tract_crosswalks, load_crosswalk_2010_2020, get_all_tract_geoms_year

## Load and validate parameters from config file 
config_file = 'config.yaml'
with open(config_file, 'r') as file: 
    config = yaml.full_load(file)
utils.validate_config(config)

# Set census parameters 
CENSUS_VARS = config['census_vars']
YEARS = range(config['start_year'], config['end_year']+1)
STATES = utils.load_state_list(config['states'])
DOWNLOAD_DIR = config['data_dir']
os.makedirs(os.path.join(DOWNLOAD_DIR, 'raw'), exist_ok=True) 
RAW_CENSUS_OUTPUT = utils.construct_raw_census_path()
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

def check_raw_census_download() -> bool: 
    """Check local file availability and config parameters before downloading raw census data."""

    proceed_download = False 
    os.makedirs(os.path.join(DOWNLOAD_DIR, 'raw'), exist_ok=True) 
    if os.path.isfile(RAW_CENSUS_OUTPUT):
        logger.info(f'Found raw census data {RAW_CENSUS_OUTPUT}')        
        if not OVERWRITE_LOCAL: 
            pass
            ## TO-DO: fix schema checks 
            # # First check if all the states in the existing output file match those currently specified in the config.yaml file  
            # df = pd.read_csv(RAW_CENSUS_OUTPUT)
            # if (df.shape[0] == 0) or any(col not in df.columns for col in ('state_name', 'year')): 
            #     logger.warning(f'Output file of same name exists already ({RAW_CENSUS_OUTPUT}), but data is missing (file empty or columns missing:\n{utils.df_to_print(df)}). Proceeding with download.')
            #     proceed_download = True  
            # else: 
            #     df_states = df['state_name'].unique()
            #     df_years = df['year'].unique()

            #     if all(var in df.columns for var in CENSUS_VARS) \
            #         and all(state in df_states for state in STATES)\
            #         and all(year in df_states for year in YEARS): 
            #             logger.info(f'Specified census variables, years, and states already downloaded in {RAW_CENSUS_OUTPUT}. Skipping download.')
            #     else: 
            #         logger.info(f'Output file of same name exists already ({RAW_CENSUS_OUTPUT}), but contains different states/years/census variables from what\'s specified. Proceeding with download.')
            #         proceed_download = True  
                
            #     # clear memory after checking file
            #     del df_states
            #     del df_years
            #     del df
        else: 
            logger.info(f"{RAW_CENSUS_OUTPUT} exists already, but `config['main']['overwrite_local']`== True. Proceeding with download.")
            proceed_download = True
    else: 
        logger.info(f'{RAW_CENSUS_OUTPUT} does not exist. Proceed with download.')
        proceed_download = True
    return proceed_download
     

def load_raw_census_data() -> pd.DataFrame: 
    """Download raw census data from Census API and return as DataFrame
        - Inserts additional columns
    """
    proceed_download = check_raw_census_download()
    if proceed_download: 
        logger.info(f'Downloading {CENSUS_VARS} from {YEARS[0]} to {YEARS[-1]}: {RAW_CENSUS_OUTPUT}')

        dataframes = []
        failed_downloads = []
        max_retries = 2

        for state in STATES: 
            n = 0 
            retries = 0    
            while (n < len(YEARS)):  
            
                raw_year_data = None
                year = YEARS[n]
        
                try: 
                
                    start_time = dt.datetime.utcnow()
                    
                    raw_year_data = c.acs5.state_county_tract(fields = ['NAME'] + CENSUS_VARS,
                                                        state_fips = state['fips'], 
                                                        county_fips = "*",
                                                        tract="*",
                                                        year=year) # will need to separate 2020 from pre-2020 (tracts w/ same GEOID are not equivalent)
                    df = pd.DataFrame(raw_year_data)
                    
                    # Insert additional columns
                    df['year'] = year
                    df['state_usps'] = state['usps']                


                    dataframes.append(df)
                    n += 1 
                    retries = 0

                    end_time = dt.datetime.utcnow()

                    elapsed_time = np.round((end_time - start_time).seconds, 2)

                    logger.info(f"Downloaded ({state['usps']}, {str(year)}, {elapsed_time}s, {df.shape[0]} rows, {df.memory_usage(deep=True).sum() / (1024 ** 2)}MB)")
                    logger.info(f'{utils.df_to_print(df.head(1))}')

                except Exception as e:
                    retries += 1
                    logger.warning(f"({state['usps']}, {str(year)}): {str(e)} (Retrying {retries}/{max_retries})")
            
                if (raw_year_data is None) and (retries == max_retries):
                    logger.warning(f"Failed download {retries}/{max_retries} -- skipping to next year")
                    n += 1 
                    retries = 0 
                    failed_downloads.append({'state_name':state['name'], 'year':year, 'vars':CENSUS_VARS}) 

        # Log successes/failures
        df = pd.concat(dataframes)
        logger.info(f"Total Download: {df.shape[0]} rows, {df.memory_usage(deep=True).sum() / (1024 ** 2)}MB")
        if len(failed_downloads) > 0: 
            failed_downloads_print = "\n".join(failed_downloads)
            logger.warning(f'{len(failed_downloads)} failed downloads:\n{failed_downloads_print}')

        # Store raw census download 
        df.to_csv(RAW_CENSUS_OUTPUT)

    else: 
        df = pd.read_csv(RAW_CENSUS_OUTPUT, index_col=0)

    return df

def _transform_raw_data_long(raw_df:pd.DataFrame) -> pd.DataFrame: 
    """Transform the data pulled from the Census API before applying the years crosswalk (retain long format)."""

    df = raw_df.copy()

    logger.debug(f'Raw Census data in {RAW_CENSUS_OUTPUT}:\n{utils.df_to_print(df)}{df.shape}\n')

    # Set data types
    df.dtypes

    # Split/edit location columns:  
    df[['tract_dec', 'county_name', 'state_name']] = df['NAME'].str.split(', ', expand=True)
    
    # Rename columns 
    df.rename({"county":'county_fips',
               'state':'state_fips', 
               'tract':'tract_fips'}, axis=1, inplace=True) 
    
    # Standardize fips columns & Construct GEOID: 
    for geog in ('state', 'county', 'tract'): 
        df[f'{geog}_fips'] = df[f'{geog}_fips'].apply(lambda x: utils.std_fips(fips_code=x, geog=geog))
    df['GEOID'] = df['state_fips'] + df['county_fips'] + df['tract_fips'] 

    
    # Strip words/spaces 
    df['tract_dec'] = df['tract_dec'].str.lstrip('Census Tract ').str.strip()
    df['county_name'] = df['county_name'].str.rstrip('County').str.strip()
    
    # Dropping/reordering columns 
    df = df[['GEOID', 'state_fips', 'state_name', 'state_usps', 'county_fips', 'county_name', 'tract_fips', 'tract_dec', 'year'] + CENSUS_VARS]

    # Handle NaN values:  
    # Negative values are sometimes used to signal missing data -- replacing these with NaNs
    rows_w_negative = df[df[CENSUS_VARS].apply(lambda row: any(val < 0 for val in row.values), axis=1)]
    n_rows_w_negative = rows_w_negative.shape[0]
    if n_rows_w_negative > 0: 
        logger.debug(f'{n_rows_w_negative} rows have negative placeholder values for census variables:\n {utils.df_to_print(rows_w_negative.value_counts().reset_index())}')
        logger.info(f'Replacing these values with NaN.')
        df[CENSUS_VARS] = df[CENSUS_VARS].applymap(lambda x: np.nan if x < 0 else x)
    nan_counts = df.isna().sum()
    logger.debug(f'NaN counts:\n {utils.df_to_print(nan_counts)}') # TO-DO: Not printing correctly 

    # Handle Duplicates
    dups = df[df.duplicated()]
    if dups.shape[0] > 0: 
        logger.info(f'{dups.shape[0]} duplicate rows (dropping)')
        df = df[~df.index.isin(dups.index)]

    logger.info(f'Transformed Census data (Long):\n{utils.df_to_print(df)}\n{df.shape}')
    return df

def _widen_df(long_df:pd.DataFrame) -> pd.DataFrame: 
    df = long_df.copy()
    df = pd.pivot(data=df, 
                index=df.filter(regex='GEOID|state|tract|county').columns,
                columns=['year'], 
                values=CENSUS_VARS)
    
    # Fix column names 
    df.columns = ['-'.join((cvar, str(year))) for (cvar, year) in df.columns]

    logger.info(f'Widened data: \n{utils.df_to_print(df)}\n{df.shape}')

    return df 


def bin_variables(wide_df:pd.DataFrame):
    """Bin each census variable per year in the widened dataframe (depending on config parameters)""" 

    ## TO-DO: test the bin validation steps in utils.validate_config()    
    logger.info(f'Checking binning parameters ({config["bins"]})')
    state_bins = config['bins']['state'] if isinstance(config['bins']['state'], int) else None
    nat_bins = config['bins']['national'] if isinstance(config['bins']['national'], int) else None

    if (state_bins is None) and (nat_bins is None): 
        logger.info('Skipping binning.')
        return wide_df
    else:
        # Bin by state first -- reduce amount of state loops/filters vs. doing it inside the column loop, 
        # though we have to loop through the columns twice this way. 
        if state_bins is not None: 
            dataframes = []
            # Get unique state_names from multi-index
            for state_name in wide_df.index.get_level_values(level='state_name').unique():     
                # Access rows with specific state name in multi-index 
                state_df = wide_df.xs(key=state_name, level='state_name', drop_level=False)
                for col in state_df.columns: # column format: <cvar>-<year>
                    state_df[f'{col}_state-bin'] = pd.qcut(state_df[col], q=state_bins)\
                            .apply(lambda b:f"{int(b.left):,} <= {int(b.right):,}")
                dataframes.append(state_df)
            binned_df = pd.concat(dataframes)
        else: 
            binned_df = wide_df.copy() 
        # Bin by national
        if nat_bins is not None: 
            for col in state_df.columns: # second column loop
                binned_df[f'{col}_nat-bin'] = pd.qcut(binned_df[col], q=nat_bins)\
                    .apply(lambda b:f"{int(b.left):,} <= {int(b.right):,}")
        # Sort columns alphabetically
        cols_sorted = sorted(binned_df.columns)
        binned_df = binned_df[cols_sorted]

        ## TO-DO: Log bins for each state, census variable, and year  
        # state_df[f'{col}_state-bin'] includes bins for all states

        return binned_df 


def _extract_2020_data(wide_df:pd.DataFrame) -> pd.DataFrame: 
    """Remove the 2020 tracts and data from the widened data. Since a tract in 2020 can have the same GEOID as a tract
    with a different boundary from the previous redistricting cycle, values before/after SOY 2020 do not really belong in the same JSON structure
    before applying the crosswalk. After widening/collapsing the pre-2020 tract data and assigning it to 2020 tract, we will re-join the 2020 data to it.  
    """

    df_2020 = wide_df.filter(regex='2020')
    wide_df.drop(df_2020.columns, axis=1, inplace=True)

    # Drop nans (TO-DO: Write functions to monitor/log where nans) are being introduced vs. raw from census API

    return df_2020.reset_index()


def _collapse_df(df_to_collapse:pd.DataFrame) -> pd.DataFrame:
    """Collapse the census variable columns for pre-2020 tracts.
    `nan_fill` -- JS apparently can't parse NaN values from a JSON, 
    """
    df = df_to_collapse.copy()

    for var in CENSUS_VARS: 
        logger.info(var)
        var_year_columns = df.filter(regex=var).columns
        df[var] = df[var_year_columns] \
            .apply(lambda row: {str(col.split('-')[1]): np.round(row_value, 2) for col, row_value in row.items()}
                if not all(np.isnan(row.values)) else np.nan, axis=1)
        df.drop(var_year_columns, axis=1, inplace=True)

    logger.info(f'Collapsed pre-2020 data: \n{utils.df_to_print(df)}\n{df.shape}')
    return df


def join_crosswalk(collapse_df:pd.DataFrame, how='inner') -> pd.DataFrame: 
    """Join the crosswalk/pct-overlap column to the widened dataframe and multiply the census variable columns by their weights"""

    # Load crosswalk from 2010 to 2020
    df_map_10_to_20 = pd.DataFrame(load_crosswalk_2010_2020(reverse=False)).set_index(['GEOID_TRACT_10']).drop(['STATENAME'], axis=1)
    
    joined = collapse_df.merge(df_map_10_to_20, how=how, left_on=['GEOID'], right_index=True)

    # Drop nans (TO-DO: Write functions to monitor/log where nans) are being introduced vs. raw from census API


    return joined 


def multiply_pct_overlaps(values_2010, weights_2020) -> pd.DataFrame: 
    """Create historical data for 2020 Tracts by multiplying past year's values by their respective crosswalk weights
    Currently works only on one variable at a time.

    Args: 

    values_2010 (pd.Series): The column of raw historical values nested as a JSON struct ({<year_n>:<variable_value>, ...})
    
    weights_2020 (pd.Series): The column of percentage weights_2020 with 2020 tract GEOIDs ({<GEOID_TRACT_20_n>:<percent_overlap>, ...})
        (joined by join_crosswalk)

    Returns: 

    df_crosswalk (pd.DataFrame): Historical yearly values for the overlapping 2020 tracts.
    
    """

    output_dict = {}
    for val_10_dict, ov in zip(values_2010, weights_2020):
        # Skip if the raw values column is nan for this 2020 tract (i.e. we couldn't obtain data for this variable and year in the Census API)
        if not isinstance(val_10_dict,dict): 
            continue
        # logger.debug(val_10_dict)
        # logger.debug(ov)
        else: 
            for tract_2020, pct in ov.items(): 
                # Convert the historical values of the given pre-2010 tract to its equivalent 2020 census tract 
                pct = pct if pct <= 1 else pct / 100
                converted_values_2010 = {year:np.round((val*pct),2) for year, val in val_10_dict.items()}
                # Add values to the output dictionary 
                if tract_2020 in output_dict.keys(): 
                    # Add to the values in the current dictionary
                    for year in output_dict[tract_2020].keys():
                        if not np.isnan(converted_values_2010[year]):
                            output_dict[tract_2020][year] += converted_values_2010[year]
                else: 
                    output_dict[tract_2020] = converted_values_2010
        
    return output_dict


def apply_crosswalk(joined): 
    """Apply the crosswalk in the joined dataframe for each census variable column"""
    dataframes = []
    for cvar in CENSUS_VARS:
        var_df = pd.DataFrame(multiply_pct_overlaps(joined[cvar],joined['GEOID_TRACT_20_overlap'])).T.rename_axis('GEOID_TRACT_20')
        var_df.columns = [cvar + "-" + str(col) for col in var_df.columns]
        dataframes.append(var_df)
        
    df = pd.concat(dataframes, axis=1)
    # Collapse
    df = _collapse_df(df)
    return df


def rejoin_2020(weighted_df:pd.DataFrame, df_2020:pd.DataFrame):
    """Rejoin the dataframe from 2020 to the crosswalked dataframe """
    # # Join dataframes 
    # joined = df_2020.merge(weighted_df, how='right', left_on=['GEOID'], right_on=['GEOID_TRACT_20']).copy()
    
    # # Insert 2020 values into the nested rows for the correct GEOID
    # for _, row in joined.iterrows():
    #     for cvar in config['census_vars']: 
    #         if isinstance(row[cvar], dict): # i.e. if not nan
    #             joined.at[row.name, cvar]['2020'] = row[f'{cvar}-2020']
    
    # # Drop the 2020 specific columns 
    # joined.drop([f'{cvar}-2020' for cvar in config['census_vars']], axis=1, inplace=True)
    

    # joined = df_2020.merge(weighted_df, how='right', left_on=['GEOID'], right_on=['GEOID_TRACT_20'])
    # for cvar in config['census_vars']:
    #     joined[cvar]['2020'] = joined[f'{cvar}-2020']
    
    joined = df_2020.merge(weighted_df, how='right', left_on=['GEOID'], right_on=['GEOID_TRACT_20'])

    def update_dict(row, cvar):
        cvar_value_2020 = row[f'{cvar}-2020']
        current_dict = row[cvar]
        current_dict['2020'] = cvar_value_2020
        return current_dict
    
    for cvar in config['census_vars']:
        joined[cvar] = joined.apply(lambda row: update_dict(row, cvar), axis=1)
        # drop the existing 2020 column from the join 
        joined.drop([f'{cvar}-2020'], axis=1, inplace=True)
        
    return joined

def join_geoms(rejoin_df:pd.DataFrame, py_geoms:pd.DataFrame): 
    """Join the geoms to the finalized dataframe"""

    # Rename columns from pygris to match our transformed census datafame
    py_geoms.rename({'STATEFP': 'state_fips',
                    'COUNTYFP': 'county_fips', 
                    'TRACTCE': 'tract_fips', 
                    'NAME': 'tract_dec'}, axis=1, inplace=True)
    # Keep only needed columns (drop 'NAMELSAD', 'MTFCC', 'FUNCSTAT')
    shared_columns = ['state_fips', 'county_fips', 'tract_fips', 'tract_dec']
    py_geoms = py_geoms[shared_columns + ['ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON', 'geometry']]
    # Keep only states from config.yaml
    states_fips_to_keep = [s['fips'] for s in STATES]
    py_geoms = py_geoms[py_geoms['state_fips'].isin(states_fips_to_keep)]

    # Join
    df_geoms = rejoin_df.merge(py_geoms, how='right', left_on='GEOID', right_index=True)

    return df_geoms


def main() -> None:
    
    ## Download/load crosswalk and pygris geoms 
    logger.info(f'Obtaining tract crosswalks (tract_crosswalk.py)')
    get_tract_crosswalks() 

    logger.info(f'Obtaining simplified geometries from pygris')
    py_geoms = get_all_tract_geoms_year(year=2020, erase_water=False, simplify_tolerance=.001)
    # get_tract_crosswalks uses raw (unsimplified) geometries to calculate overlaps -- these are for our final output here

    ## Download (or load cached) raw census data
    df = load_raw_census_data()

    ## Transform raw data (long format)
    df = _transform_raw_data_long(df)

    ## Widen data 
    df = _widen_df(df)

    ## Apply binning (if parameters set)
    df = bin_variables(df)
    
    ## Separate 2020 data from other years
    df_2020 = _extract_2020_data(df)

    ## Collapse pre-2020 data 
    df = _collapse_df(df)

    ## Join and apply the crosswalk column to the pre-2020 data
    df = join_crosswalk(df)
    df = apply_crosswalk(df)

    ## Re-Join the 2020 data to crosswalked pre-2020 data
    df = rejoin_2020(df, df_2020)

    ## Join the geometries 

    # Rename columns from pygris to match our transformed census datafame
    py_geoms.rename({'STATEFP': 'state_fips',
                     'COUNTYFP': 'county_fips', 
                     'TRACTCE': 'tract_fips', 
                     'NAME': 'tract_dec'}, axis=1, inplace=True)
    shared_columns = ['state_fips', 'county_fips', 'tract_fips', 'tract_dec']
    # Keep only needed columns (drop 'NAMELSAD', 'MTFCC', 'FUNCSTAT')
    py_geoms = py_geoms[shared_columns + ['ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON', 'geometry']]
    # Keep only states from config.yaml
    states_fips_to_keep = [s['fips'] for s in STATES]
    py_geoms = py_geoms[py_geoms['state_fips'].isin(states_fips_to_keep)]

    # Join
    df_geoms = df.merge(py_geoms, how='right', left_on='GEOID', right_index='GEOID')
    # df_geoms = df_geoms[~(df_geoms['GEOID'].isna()) & ~(df_geoms['geometry'].isna())] # TO-DO: Handle/Monitor/Track NaNs
        
    ## Save output 
    # Convert to geodataframe
    df_geoms = gpd.GeoDataFrame(df_geoms)

    # Replace all nans with 'NaN' for GeoJSON so JS can parse
    df_geoms.fillna(value='NaN', inplace=True)
    for cvar in CENSUS_VARS: 
        df_geoms[cvar] = df_geoms[cvar].apply(lambda x: utils.replace_dict_nans(x))

    output_path = utils.construct_geojson_output_path() # TO-DO: Change path so it includes the simplification tolerance, water erasure, and buffer (if applicable)
    logger.info(f'Saving to {output_path}')
    df_geoms.to_file(output_path)

if __name__ == "__main__":
    main()