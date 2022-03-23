import pandas as pd
import requests
import geopandas
import json
import plotly.express as px
import matplotlib.pyplot as plt
from shapely import wkt


def get_conversion_table():
    # get conversion  table from the census website
    conversion_table = requests.get(
        'https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_st10.txt')
    lines = conversion_table.text.splitlines()
    # fix typo
    lines[0] = lines[0].replace('ï»¿', '')

    census_tract_conversion_list = []
    headers = lines.pop(0).split('|')
    for line in lines:
        my_dict = {}
        line = line.split('|')
        for cat in headers:
            index = headers.index(cat)
            my_dict[cat] = line[index]
        census_tract_conversion_list.append(my_dict)
    return census_tract_conversion_list


def tract_finder(data, geoid, start_year, end_year):
    results_dict = {}
    if (start_year == 2010) and (end_year == 2020):
        list_of_equivalent_tarcts = []
        GEOID_TRACT_10_index = []
        for (index, d) in enumerate(data):
            if d["GEOID_TRACT_10"] == geoid:
                GEOID_TRACT_10_index.append(index)
        if len(GEOID_TRACT_10_index) == 0:
            text = 'NA in 2010'
            list_of_equivalent_tarcts.append(text)
        for idx in GEOID_TRACT_10_index:
            list_of_equivalent_tarcts.append(data[idx]['GEOID_TRACT_20'])
        results_dict['GEOID_TRACT_10'] = geoid
        results_dict['GEOID_TRACT_20'] = list_of_equivalent_tarcts
        return results_dict
    elif (start_year == 2020) and (end_year == 2010):
        list_of_equivalent_tarcts = []
        GEOID_TRACT_20_index = []
        for (index, d) in enumerate(data):
            if d["GEOID_TRACT_20"] == geoid:
                GEOID_TRACT_20_index.append(index)
        for idx in GEOID_TRACT_20_index:
            list_of_equivalent_tarcts.append(data[idx]['GEOID_TRACT_10'])
        results_dict['GEOID_TRACT_20'] = geoid
        results_dict['GEOID_TRACT_10'] = list(set(list_of_equivalent_tarcts))
        return results_dict
    else:
        print('enter valid year')


def overlap_percentage(parent_geoid, child_geoid, start_year, end_year, start_year_shp, end_year_shp):
    """
    Parameters:
    parent_geoid = the GEOID for parent tract (str)
    child_geoid = the GEOID for child tract list((str))
    start_year = Year for the parent GEOID 2010 or 2020 (int)
    end_year = Year for the child GEOID 2010 or 2020 (int)
    start_year_shp = path for the start year shape file
    end_year_shp = path for the end year shape file

    """
    if start_year == 2010 and end_year == 2020:
        shp_path_20 = end_year_shp
        shp_path_10 = start_year_shp
        tract10_gdf = geopandas.read_file(shp_path_10)
        tract20_gdf = geopandas.read_file(shp_path_20)
        geoid_2020 = child_geoid
        geoid_2010 = parent_geoid
        dict_re = {}
        dict_re['GEOID_TRACT_10'] = geoid_2010
        parents_list = []
        for geoid in geoid_2020:
            parents = {}
            idx = tract10_gdf.index[tract10_gdf['GEOID10'] == geoid_2010]
            idx2 = tract20_gdf.index[tract20_gdf['GEOID'] == geoid]
            # print(idx,idx2)
            geom_2010 = tract10_gdf['geometry'].iloc[idx[0]]
            geom_2020 = tract20_gdf['geometry'].iloc[idx2[0]]
            persenatge = (geom_2010.intersection(geom_2020).area / geom_2010.area) * 100
            parents['GEOID_TRACT_20'] = geoid
            parents['GEOID_TRACT_20 overlap %'] = persenatge
            parents_list.append(parents)
        dict_re['parents'] = parents_list
    if start_year == 2020 and end_year == 2010:
        shp_path_10 = end_year_shp
        shp_path_20 = start_year_shp
        tract10_gdf = geopandas.read_file(shp_path_10)
        tract20_gdf = geopandas.read_file(shp_path_20)
        geoid_2010 = child_geoid
        geoid_2020 = parent_geoid
        dict_re = {}
        dict_re['GEOID_TRACT_20'] = geoid_2020
        parents_list = []
        for geoid in geoid_2010:
            parents = {}
            idx = tract10_gdf.index[tract10_gdf['GEOID'] == geoid]
            idx2 = tract20_gdf.index[tract20_gdf['GEOID'] == geoid_2020]
            # print(idx,idx2)
            geom_2010 = tract10_gdf['geometry'].iloc[idx[0]]
            geom_2020 = tract20_gdf['geometry'].iloc[idx2[0]]
            persenatge = (geom_2010.intersection(geom_2020).area / geom_2010.area) * 100
            parents['GEOID_TRACT_10'] = geoid
            parents['GEOID_TRACT_10 overlap %'] = persenatge
            parents_list.append(parents)
    dict_re['parents'] = parents_list
    return dict_re


if __name__ == "__main__":
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
    # print(data[0])
    print(eq_tar)
    print(par_tar)
