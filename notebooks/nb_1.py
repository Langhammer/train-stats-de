# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# Notebook 1: Retrieving Data
# ===========================

# %%
import http.client
import json
import re
import time
import xml.etree.ElementTree as ET
from math import isnan, nan

import pandas as pd
from sklearn.metrics.pairwise import euclidean_distances

import credentials
from tsde.data import normalize_name

# %% [markdown]
# # Stations Query
# I got the list of stations from the API in JSON format.

# %%
with open('../data/raw/stations.json', encoding='utf-8') as f:
    stations_raw = json.load(f)

# %%
stations = pd.DataFrame(columns=['station_id']).set_index('station_id')
for this_station in stations_raw['stations']:
    station_id = this_station['stationID']
    stations.loc[station_id, 'name'] = this_station['names']['DE']['name']
    
    try:
        stations.loc[station_id, 'postal_code'] = this_station['address']['postalCode']
    except:
        pass
    
    try:
        stations.loc[station_id, 'city'] = this_station['address']['city']
    except:
        pass
    
    try:
        stations.loc[station_id, 'category'] = this_station['stationCategory']
    except:
        pass
    
    try:
        stations.loc[station_id, 'longitude'] = this_station['position']['longitude']
        stations.loc[station_id, 'latitude'] = this_station['position']['latitude']
    except:
        pass

# %%
stations[stations['city'].isna()]

# %%
# Source: https://data.deutschebahn.com/
eva_numbers = pd.read_csv('../data/raw/D_Bahnhof_2020_alle(1).CSV', sep=';') 

# %%
eva_numbers['name_normalized'] = eva_numbers['NAME'].apply(normalize_name)


# %%
def find_eva(name):
    """Find the corresponding eva number for the given station name"""
    try:
        return eva_numbers[eva_numbers['name_normalized']==normalize_name(name)].loc[:,"EVA_NR"].to_list()[0]
    except:
        pass

stations['name_normalized'] = stations['name'].apply(normalize_name)
stations['eva'] = stations['name'].apply(lambda x: find_eva(x))

# %%
stations.isna().sum()

# %%
stations[stations['eva'].isna()]

# %% [markdown]
# There are still quite a few stations missing. It should be possible to find them via their geolocation, but I will move on at this point and try that later.

# %%
# Calculate the distances between stations in the two dataframes
points_stations = stations[stations['eva'].isna()][['latitude', 'longitude']].dropna(how='any')
points_eva = eva_numbers[['Breite', 'Laenge']].dropna(how='any').applymap(func=(lambda x: x.replace(',', '.')))
distances = euclidean_distances(points_stations[['latitude', 'longitude']], points_eva[['Breite', 'Laenge']])

# Convert the distances to a dataframe
distances = pd.DataFrame(distances, index=points_stations.index, columns=eva_numbers['EVA_NR'])

# Melt the distances dataframe to long format
distances = distances.reset_index().melt(id_vars='station_id', var_name='eva', value_name='distance')

# Filter the data to keep only the matching stations
threshold = 0.01 # degrees, corresponding to a distance of about 1.11 km 
matches = distances[distances['distance'] < threshold].set_index('station_id')

# Check that there is not more than one matching eva_number per station
matches.value_counts()
matches

#stations
stations.loc[matches.index,'eva'] = matches['eva']

# %%
stations.loc[stations['eva'].isna(), 'category'].value_counts()

# %% [markdown]
# The eva numbers are now missing only for small stations (category 6 and 7), with few exceptions. I will fix these manually, if possible.

# %%
stations[(stations['eva'].isna()) & (stations["category"]=="CATEGORY_3")]

# %% [markdown]
# The eva number for this particular train station is not available in this dataset, I olny found a value on the hungarian Wikipedia, which I will try.

# %%
stations.loc['3729', 'eva'] = 8003693

# %% [markdown]
# ## Conversion of the *Category* variable to int
# Each station is labeled with a *category.* These categories are ordered from 1 to 7. In the original dataset, this data is saved as string, which is not a proper datatype for ordinal variables.

# %%
stations['category'].value_counts(dropna=False)

# %%
stations['category'].replace({nan: "CATEGORY_8"}, inplace=True)
stations.loc["category"] = stations["category"].apply(lambda c: c[-1]).astype('int')

# %% [markdown]
# ## Saving the stations data

# %%
stations.to_parquet('stations.parquet')
eva_numbers.to_parquet('eva_numbers.parquet')

# %% [markdown]
# # Timetable Query

# %%
conn = http.client.HTTPSConnection("apis.deutschebahn.com")

headers = {
    'DB-Client-Id': credentials.DB_CLIENT_ID,
    'DB-Api-Key': credentials.DB_API_KEY,
    'accept': "application/json"
    }

DATE = "230316"
HOUR = "09"
evano = 8000046 # Siegen: 8000046

terminals = set()
connections = pd.DataFrame(
    columns=['id', 'station_name', "type", 
             "departure_time", "line_number", 
             "path", "terminal"]).set_index("id")

queried_stations = set()
stations_to_query = set()

toc = time.time()
tic = time.time()
i_query = 0
next_terminal = "Siegen Hbf"


# %%
def query_eva(station_name, conn):
    """Queries the eva number of the specified station via the timetables API."""
    conn.request("GET", "/db-api-marketplace/apis/timetables/v1/station/" + station_name, headers=headers)
    data = conn.getresponse().read()

    # Convert bytestring to ElementTree
    root = ET.fromstring(data)
    evano = root[0].attrib['eva']
    return evano


# %%
evano = query_eva("Chemnitz%20Technopark", conn)


# %%
def collect_connections(date, hour, evano=8000046, next_terminal="Siegen Hbf",tic=1, toc=1):
    """Collect information from timetables of German train stations for a given date and hour."""
    date = str(date)
    hour = str(hour)

    toc = time.time()
    tic = time.time()
    i_query = 0
    
    while next_terminal is not None:
        if toc-tic <=1:
            time.sleep(1-(toc-tic))
        tic = time.time()
        i_query += 1
        print(i_query, next_terminal)

        # API query
        conn.request("GET", "/db-api-marketplace/apis/timetables/v1/plan/"+ str(int(evano)) +"/"+ DATE +"/" + HOUR, headers=headers)
        res = conn.getresponse()
        data = res.read()
        
        # Read XML data
        root = ET.fromstring(data)

        for journey in root:
            # Create row for journey and add station name
            station_name = root.attrib['station'].strip('\\')
            connections.loc[journey.attrib['id'], 'station_name'] = station_name
            connections.loc[journey.attrib['id'], 'eva'] = evano
            queried_stations.add(station_name)

            # The type of the journey, e.g. RB or ICE
            connections.loc[journey.attrib['id'], 'type'] = journey[0].attrib['c']        
            connections.loc[journey.attrib['id'], 'departure_time'] = journey[1].attrib["pt"]
            try:
                connections.loc[journey.attrib['id'], 'line_number'] = journey[1].attrib["l"]
            except:
                pass
            # The tag "ar" means "arrival", while "dp" stands for departure
            connections.loc[journey.attrib['id'], 'event_type'] = journey[1].tag

            # The stops of the train are separted with pipes (|)
            connections.at[journey.attrib['id'], 'path'] = journey[1].attrib["ppth"].split('|')

            # We will now find out which train station to query next.
            # We will add the last stop of a departing train and the first stop of an arriving train to our agenda.
            if connections.loc[journey.attrib['id'], 'event_type']=='dp':
                terminal = connections.loc[journey.attrib['id'], 'path'][-1]
            else:
                terminal = connections.loc[journey.attrib['id'], 'path'][0]
            connections.loc[journey.attrib['id'], 'terminal'] = terminal
            stations_to_query.add(terminal)
            stations_to_query.difference_update(queried_stations)

        # Find the next station to query
        next_station_found = False
        while not next_station_found:
            if len(stations_to_query)==0:
                print('Finished. No stations left to query.')
                return connections
            
            next_terminal = stations_to_query.pop()
            evano = stations[stations['name_normalized']==normalize_name(next_terminal)]['eva']
            if len(evano)==0:
                queried_stations.add(next_terminal)
                print('No elements in list', next_terminal)
                continue
            elif isnan(evano[0]):
                try:
                    
                queried_stations.add(next_terminal)
                print('eva is nan', next_terminal)
                continue
            else:
                next_station_found = True
                evano = evano[0]
            
        toc = time.time()

connections =  collect_connections(DATE, HOUR)


# %%
stations[stations['eva'].isna()].T


# %% [markdown]
# ## Split the id
#
# This is the explanation from https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/definitions:
# "An id that uniquely identifies the stop. It consists of the following three elements separated by dashes 
# * a 'daily trip id' that uniquely identifies a trip within one day. This id is typically reused on subsequent days. This could be negative. 
# * a 6-digit date specifier (YYMMdd) that indicates the planned departure date of the trip from its start station. 
# * an index that indicates the position of the stop within the trip (in rare cases, one trip may arrive multiple times at one station). Added trips get indices above 100. 
#
# Example '-7874571842864554321-1403311221-11' would be used for a trip with daily trip id '-7874571842864554321' that starts on march the 31th 2014 and where the current station is the 11th stop. "

# %%
def split_id(id_string):
    """Split the id of a stop into daily_trip_id, date_id and position_in_trip"""
    daily_trip_id, date_id, position_in_trip = id_string[1:].split("-")
    daily_trip_id = id_string[0] + daily_trip_id
    return daily_trip_id, date_id, position_in_trip


# %%
connections.reset_index(drop=False, inplace=True)
connections['id'] = connections['id'].apply(split_id)
connections[['daily_trip_id', 'date_id', '_position_in_trip']] = pd.DataFrame(connections['id'].to_list(), index=connections.index)
connections

# %%
connections.to_parquet('../data/processed/connections' + DATE + HOUR + '.parquet')
stations.to_parquet("../data/processed/stations.parquet")
