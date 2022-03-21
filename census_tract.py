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


if __name__ == "__main__":
    data = get_conversion_table()
    geoid = '10003016301'
    eq_tar = tract_finder(data, geoid, 2020, 2010)
    print(data[0])
    print(eq_tar)

