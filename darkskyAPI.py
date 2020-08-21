#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 09:47:00 2019

@author: dariush
"""
import pandas as pd
import numpy as np
import requests
import json
import os
import datetime as dt
from datetime import datetime
import datetime
from flatten_json import flatten

key = '[insert key]'
loc = pd.read_csv('latlongs.csv')

loc['date2'] = pd.to_datetime(loc['date'], format="%m/%d/%y")
loc['epoch'] = (pd.DatetimeIndex((loc['date2']).astype(np.int64) // 10**9))+1000

loc['date2'].apply(lambda x: x.strftime('%d%m%Y'))

loc_list = loc.values.tolist()

output_folder_path = 'darksky/'

for row in loc_list:
    #get data from row
    epoch = row[5]
    latitude = row[1]
    longitude = row[2]
    lockey = row[3]
    
    # Create URL template
    url_template = "https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,hourly,flags"
    
    # Create request URL
    request_url = url_template.format(
        key,
        latitude,
        longitude,
        epoch)
    
    # Get historical weather data as JSON 
    response = requests.get(request_url)
    
    # Handle unsuccessful response code
    if not response.ok:
        print(response.json())
        continue
    
    # Get JSON response
    json_data = response.json()
    
    # Create output JSON (cache) file
    output_json = open(
        file = output_folder_path + lockey + ".json",
        mode = "wt")
    
    # Write JSON to output file
    json.dump(
        obj = json_data,
        fp = output_json,
        indent = 4)
    
    # Close the JSON file
    output_json.close()
    
# define function that flattens then turns json file to df
def jsonToDF(file):
    with open(file, 'r') as f: data = json.load(f)
    flat = flatten(data)
    return pd.DataFrame([flat])

# create blank dataframe for json data


# read all json files into dataframe
json_files = [x for x in os.listdir(output_folder_path) if x.endswith('.json')]
for x in json_files:
    df = jsonToDF(x)
    df1 = df1.append(df)


###############################
# turn 1 file into pandas dataframe as a test & write to csv
with open('newyNY91519.json', 'r') as f:
    data = json.load(f)
flat = flatten(data)
df1 = pd.DataFrame([flat])
df.to_csv('weathersample.csv')

