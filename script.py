import json
import pandas as pd
from pandas import json_normalize


# Load JSON files and define variables
with open('data_1.json') as data_1:
    read_content_1 = json.load(data_1)
with open('data_2.json') as data_2:
    read_content_2 = json.load(data_2)
with open('data_3.json') as data_3:
    read_content_3 = json.load(data_3)

# Pull out instances from main objects and start isolating constants
constants = read_content_1[0].pop('payload')
instances_2 = read_content_2[0].pop('payload')
instances_3 = read_content_3[0].pop('payload')

# Isolating last three constant key:values ('accountuuid', 'version', 'deviceid')
instances_1 = constants.pop('data')

# Create dictionary of constant key:value pairs
constants.update(read_content_1[0])

# Remove nesting and standardize variable names
clean_instances_1 = instances_1
clean_instances_2 = instances_2.pop('data')
clean_instances_3 = instances_3.pop('data')

# Iterate and append constant key:value pairs to each instance
for i in clean_instances_1:
    i.update(constants)

for i in clean_instances_2:
    i.update(constants)

for i in clean_instances_3:
    i.update(constants)

# Concatenate dictionaries together to form one list
grouped = clean_instances_1 + clean_instances_2 + clean_instances_3

# Flatten data to format for table
flat = pd.json_normalize(grouped, max_level=0)

# Rearrange column order
final = flat[['tenantName', 'eventType', 'eventTimeEpoch', 'antenna', 'datetime', 'readername', 'epc', 'deviceid', 'user', 'tagevent', 'peakrssi', 'accountuuid', 'version']]

# Create csv file with indexes removed
final.to_csv('final_table.csv', index=None)
