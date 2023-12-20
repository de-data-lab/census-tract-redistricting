from azure.storage.blob import BlobServiceClient
import os
import us 
import yaml 
from pygris.helpers import validate_state
import json
import pandas as pd 
from logger import logger
from collections import Counter 


class AzureBlobStorageManager:
    def __init__(self, connection_str:str, container_name:str, download_dir="."):
        
        self.container_name = container_name
        
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        self.container_client = self.blob_service_client.get_container_client(container_name)

        # The default directory to which to download a blob.
        self.download_dir = download_dir

    def upload_blob(self, file_name:str,  blob_name=None, overwrite=False) -> None:
        """Upload a local file to blob storage in Azure"""

        # Default blob_name = local filename 
        if blob_name is None:
            blob_name = os.path.basename(file_name)
        blob_client = self.container_client.get_blob_client(blob_name)
        
        try:
            # Upload the blob
            with open(file_name, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
            print(f"Blob {blob_name} uploaded successfully.")
        except Exception as e: # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")

    def list_blobs(self, name_only=True) -> list: 
        """Wrapper to list blobs in the container"""
        blob_list = self.container_client.list_blobs()
        if name_only: 
            return [blob.name for blob in blob_list]
        else: 
            return list(blob_list)

    def download_blob(self, blob_name:str, download_path=None): 
        """Download a blob from the container. Local download path defaults to blob_name"""

        blob_client = self.container_client.get_blob_client(blob_name)

        if download_path is None:
            download_path = os.path.join(self.download_dir, os.path.basename(blob_name)) 
        
        with open(download_path, "wb") as file:
            download_bytes = blob_client.download_blob().readall()
            file.write(download_bytes)

    def has_blob(self, file_name:str) -> bool: 
        """Check if the container has a blob of the given name"""

        return os.path.basename(file_name) in self.list_blobs(name_only=True) 
            

def load_state_list(states=['All'], include_dc=True, include_pr=False) -> list: 
    """Load list of states (Names, FIPS, USPS) from us module
    
    Args: 

    states (list): List of valid US state designations (defaults to ['All'] to load all states)

    include_dc (bool): Whether to also include DC in the list of states (default=True). 
        Ignored if a custom list of states is given for 'states'.

    include_pr (bool): Whether to also include PR in the list of states (default=False). 
        Ignored if a custom list of states is given for 'states'.

    Returns: 
    
    state_list (list): List of US states by name, FIPS code, and USPS abbreviation
    
    """
    if states == ['All']: 
        state_list = [{"name":s.name, "fips":s.fips, 'usps':s.abbr} for s in us.states.STATES]
        if include_dc: 
            state_list += [{'name':'District of Columbia', 'fips':'11', 'usps':'DC'}]
        if include_pr: 
            state_list += [{'name':'Puerto Rico', 'fips':'72', 'usps':'PR'}]
    else: 
        state_list = []
        invalid_states = []
        for s in states: 
            try: 
                # Validate state with pygris helper function (returns fips if valid state)
                s_fips = validate_state(s, quiet=True)
                # Use fips to access State object from us.states
                s_state_obj = us.states.lookup(s_fips) 
                # Add state information to output
                state_list.append({'name':s_state_obj.name, 'fips':s_state_obj.fips, 'usps':s_state_obj.abbr})
            except ValueError as e: 
                invalid_states.append(s)
        if len(invalid_states) > 0: 
            raise ValueError(f'Invalid states provided: {invalid_states}')
    
    return state_list


def _param_sum_str(include_cvars=True, include_states=True, include_years=True) -> str: 
    """Summarize parameters in config (for constructing file paths)."""

    with open('config.yaml', 'r') as file: 
        config = yaml.full_load(file)

    param_sum_str = ''

    if include_cvars: 
        param_sum_str += '-'.join(config['census_vars'])

    if include_states: 
        state_list = load_state_list()
        
        if config['states'] == ['All']: 
            state_str = 'allStates'
        elif len(config['states']) < 8:
            state_str = '-'.join([s['usps'] for s in state_list if any(s[k] in config['states'] for k in ('name', 'fips', 'usps'))])
        else: 
            state_str = f"{len(config['states'])}-States"      

        if config['include_dc']: 
            state_str += '+DC'
        if config['include_pr']: 
            state_str += '+PR'

        param_sum_str += "_" + state_str

    if include_years:
        param_sum_str += f'_{config["start_year"]}-{config["end_year"]}'

    # param_sum_str = state_str + f'_{config["start_year"]}-{config["end_year"]}'
    return param_sum_str


def construct_raw_census_path() -> str: 
    """Construct the path for the data downloaded from the Census API for the variables and years specified in config.yaml"""

    with open('config.yaml', 'r') as file: 
        config = yaml.full_load(file)

    return os.path.join(config['data_dir'], 'raw', _param_sum_str() + '.csv')

def construct_geojson_output_path() -> str: 
    """Construct the output path for the geojson produced by census_data.py based on config.yaml"""

    with open('config.yaml', 'r') as file: 
        config = yaml.full_load(file)

    return  os.path.join(config['data_dir'], _param_sum_str() +'.json')


def validate_config(config:dict) -> dict:
    """Validate the parameter values in config.yaml
    
    Args: 

    config (dict): Dictionary representation of the config.yaml file

    Returns:

    error_messages (dict): Dictionary of parameters in config.yaml with validation issues.  

    """

    error_messages = {}

    with open('config.yaml', 'r') as file: 
        config = yaml.full_load(file)

    # census variable selection
    if (not isinstance(config['census_vars'], list)) or (len(config['census_vars']) == 0): 
        error_messages['census_vars'] = 'Provide non-empty list of valid census variables.'
    dup_vars = [k for k,v in Counter(config['census_vars']).items() if v > 1]
    if len(dup_vars) > 0:
        error_messages['census_vars'] = f'Enter each census variable only once ({dup_vars})'

    # year selection
    if any(config[y] not in range(2010, 2021) for y in ('start_year', 'end_year')): 
        error_messages['census_vars'] = 'Must be valid years from 2010 to 2020.'

    # state selection 
    if (not isinstance(config['states'], list)) or (len(config['states']) == 0): 
        error_messages['states'] = 'Provide non-empty list of valid US states.' 
    elif (config['states'] != ['All']): 
        invalid_states = []
        for s in config['states']: 
            try: 
                validate_state(s, quiet=True)
            except ValueError as e: 
                invalid_states.append(s)
        if len(invalid_states) > 0:
            error_messages['states'] = f'{invalid_states}'

    # log results
    if len(error_messages.items()) > 0: 
        logger.info(error_messages)
        for param, error in error_messages.items(): 
            logger.error(f'config.yaml -- {param}: {error}')
        raise Exception(f"Revise parameters in config.yaml")
    
    return error_messages


def read_json_rows(fp:str) -> list:
    """Read JSON row file to list of dictionaries"""
    output = []
    with open(fp, 'r') as file: 
        for line in file: 
            json_row = json.loads(line.strip())
            output.append(json_row)
    return output

def df_to_print(df:pd.DataFrame, n_rows=5, index=False) -> str: 
    """Write a pd.DataFrame to CSV string but with spaces between commas for clarity"""
    output_str = ""
    for line in df.to_csv(index=index).splitlines()[:min(n_rows, df.shape[0])]: 
        output_str += line.replace(',', ', ') + "\n"
    return output_str



def std_fips(fips_code, geog=None) -> str:
    """Standardize a FIPS code based on what level geography it represents. Supports State, County, Tract, and Block.

    Args: 

    fips_code (float, int, numeric type str): The FIPS code to standardize 

    geog (str): The geography level of the FIPS code ("state", "county", "tract", "block")

    Returns:

    fips_std (str): A standardized str version of the FIPS code. Standardization involves removing trailing decimals and the following zero-fill rules:

        "state": 2-digit zero fill 
        "county": 3 digits
        "tract": 6 digits
        "block": 4 digits 

    Errors:
    If geog not a valid geography level ("state", "county", "tract", "block")
    IF the FIPS code is not a valid numeric type.
    If the FIPS code has non-zero trailing digits after a decimal.  
    If the FIPS code has more digits than the standard number for the specified geography. 

    Example Usage:

    df['STATEFP'] = df['STAFEP'].apply(std_fips(geog="state"))
    """

    n_digits = {"state":2, "county":3, "tract":6, "block":4}

    
    if geog is None or geog not in n_digits.keys(): 
        raise Exception('Must specify valid value for geog ("state", "county", "tract", "block")')
    
    fips_str = str(fips_code).strip()

    ## TO-DO: fix numeric type validation
    # if not fips_str.isnumeric():
    #     logger.info(fips_str.isnumeric())
    #     logger.info(fips_str)
    #     logger.info(f'type: {type(fips_code)}')
    #     raise Exception(f"{fips_code} is not a valid numeric type")
    # elif len(fips_str) > n_digits[geog]:
    #     raise Exception(f"{fips_code} has too many digits for a {geog}-level FIPS code ({n_digits[geog]})")

    # decimal_index = fips_str.find(".")
    # if decimal_index != -1:
    #     if any(x != "0" for x in fips_str[decimal_index:]): 
    #         raise Exception(f"{fips_code} has non-zero trailing digits")
    #     else: 
    #         fips_str = fips_str[:decimal_index]
    
    return fips_str.zfill(n_digits[geog])


def validate_transformation(): 
    """Validate the outcome of a dataframe transformation"""


def nan_counts(df): 
    df.isna().sum()


def replace_dict_nans(d:dict, nan_fill='NaN'):
    """Replace nans in a dictionary with None"""
    if isinstance(d, dict):
        return {k: (v if not pd.isna(v) else nan_fill) for k, v in d.items()}
    else:
        return d
