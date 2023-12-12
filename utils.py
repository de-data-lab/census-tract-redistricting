from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import us 
import yaml 
import re 
from pygris.helpers import validate_state

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
                validate_state(s)
            except ValueError as e: 
                invalid_states.append(s)
        else: 
            error_messages['states'] = f'Invalid states provided ({invalid_states}).'
    
    return error_messages