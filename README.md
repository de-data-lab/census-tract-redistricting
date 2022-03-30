# census-tract-redistricting

This repo is dedicated to each one of the individual scripts that will represent different processes for census tract  
walkthroughs/conversions. 

In the US, the Census Bureau re-draws districts every 10 years. 

## Running Procedure  
1. Initialize the conversion table 
2. Get the GEOID you want to process 
3. Specify the setup parameters in order
   1. parent_geoid = the GEOID for parent tract (str)
   2. child_geoid = the GEOID for child tract list((str))
   3. start_year = Year for the parent GEOID 2010 or 2020 (int)
   4. end_year = Year for the child GEOID 2010 or 2020 (int)
   5. start_year_shp = path for the start year shape file 
   6. end_year_shp = path for the end year shape file

## Example
The example below illustrates the usage for the script to find the equivalent tracts in   
2010 for GEOID: 10001043202 

    data = get_conversion_table()
    geoid = '10001043202'
    start_year = 2020
    end_year = 2010
    eq_tar = tract_finder(data, geoid, start_year, end_year)
    start_year_tract = eq_tar['GEOID_TRACT_20']
    end_year_tract = eq_tar['GEOID_TRACT_10']
    start_year_shapefile = 'tl_2020_10_tract'
    end_year_shapefile = 'tl_2018_10_tract'
    par_tar = overlap_percentage(start_year_tract,
                                 end_year_tract,
                                 start_year,
                                 end_year,
                                 start_year_shapefile,
                                 end_year_shapefile)
    print(eq_tar)
    print(par_tar)

