# census-tract-redistricting

This branch contains scripts for downloading US Census data from multiple years at the census tract level and converting data from previous years' tracts to current tracts based on area overlap. The code in this branch is entirely different from main, as this portion of the original repo was basically not started yet.

* *Currently supported years*: 2010-2020 (i.e. converts any tract data from 2010-2019 to 2020 tracts)
* Might extend support back to earlier years if USCB provides the necessary files (like [these](https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/) for 2010-2020)

```bash 
├── README.md
├── config.yaml
├── example-use-notebook.ipynb
├── tract_crosswalk.py
├── main.py
├── logger.py
├── utils.py
└── requirements.txt
```

#### *config.yaml*

Set all download parameters here. See comments for parameter descriptions.

To find the Azure connection string and container name, search the Tech Impact Azure account for `pipelinemapping`.

* (If that's ever deleted at some point, you can just create a new storage account and container and add those names + the connection string to `config.yaml` instead)

#### *tract_crosswalk.py*
Downloads the tract area crosswalk. The USCB has [files](https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/) explaining which tracts from 2010 overlap with tracts from 2020, but it does not state how much the tracts overlap. So while this file is essential to narrow down the amount of calculations we need to do, we still need to calculate the overlaps ourselves.

* If the complete crosswalk file exists in Azure and `overwrite_azure: False`, the script will download it from there rather than re-producing it from scratch.  
* If re-producing the file, the script will upload to the provided Azure container. 
* Currently the script will not run unless a valid Azure container is provided (this is intentional for team version control).

#### *main.py*
Downloads tract-level data from the US Census API and uses the crosswalk file to map data from past-years tracts to current-year tracts. 
* Specify desired census variables, states, and years in `config.yaml`

