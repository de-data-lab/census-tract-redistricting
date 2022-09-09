import pandas as pd
import requests


def tract_finder(data, geoid, start_year, end_year):
    retults_dict = {}
    if (start_year == 10) and (end_year == 20):
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
        retults_dict['GEOID_TRACT_10'] = geoid
        retults_dict['GEOID_TRACT_20'] = list_of_equivalent_tarcts
        return retults_dict
    elif (start_year == 20) and (end_year == 10):
        list_of_equivalent_tarcts = []
        GEOID_TRACT_20_index = []
        for (index, d) in enumerate(data):
            if d["GEOID_TRACT_20"] == geoid:
                GEOID_TRACT_20_index.append(index)
        for idx in GEOID_TRACT_20_index:
            list_of_equivalent_tarcts.append(data[idx]['GEOID_TRACT_20'])
        retults_dict['GEOID_TRACT_10'] = geoid
        retults_dict['GEOID_TRACT_20'] = list(set(list_of_equivalent_tarcts))
        return retults_dict
    else:
        print('enter vaild year')


if __name__ == "__main__":
    # get converstion table from the census website
    converstion_table = requests.get(
        'https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_st10.txt')
    lines = converstion_table.text.splitlines()
    # fix typo
    lines[0] = lines[0].replace('ï»¿', '')

    census_tract_converstion_list = []
    headers = lines.pop(0).split('|')
    for line in lines:
        my_dict = {}
        line = line.split('|')
        for cat in headers:
            index = headers.index(cat)
            my_dict[cat] = line[index]
        census_tract_converstion_list.append(my_dict)

    list_of_10_DE_tracts = []
    list_of_10_DE_tracts_names = []
    list_of_20_DE_tracts = []
    list_of_20_DE_tracts_names = []

    for (index, d) in enumerate(census_tract_converstion_list):
        list_of_10_DE_tracts.append(d["GEOID_TRACT_10"])
        list_of_10_DE_tracts_names.append(d["NAMELSAD_TRACT_10"])
        list_of_20_DE_tracts.append(d["GEOID_TRACT_20"])
        list_of_20_DE_tracts_names.append(d["NAMELSAD_TRACT_20"])

    tarct_converstion_list = []
    for tract in set(list_of_10_DE_tracts):
        tarct_converstion_list.append(tract_finder(census_tract_converstion_list, tract, 10, 20))
    df = pd.DataFrame(tarct_converstion_list)
    # reformat (218X8)
    df2 = pd.DataFrame(df['GEOID_TRACT_20'].to_list(), columns=['GEOID_TRACT_20',
                                                                'GEOID_TRACT_20',
                                                                'GEOID_TRACT_20',
                                                                'GEOID_TRACT_20',
                                                                'GEOID_TRACT_20',
                                                                'GEOID_TRACT_20',
                                                                'GEOID_TRACT_20'])
    # add 2010 tracts back
    geoid = df['GEOID_TRACT_10']
    df2.insert(loc=0, column='GEOID_TRACT_10', value=geoid)
    # print(df2)

    # uncomment if you want to save csv file
    #df2.to_csv('DE_tarcts_converstion_table.csv')
