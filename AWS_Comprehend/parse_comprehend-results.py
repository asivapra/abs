#!/usr/bin/env python

import csv
import json

# def make_json(csvFilePath, jsonFilePath):
#     """
#     {
#         "1": {
#             "{\"Entities\": [{\"BeginOffset\": 0": "{\"Entities\": [{\"BeginOffset\": 0",
#             " \"EndOffset\": 6": " \"EndOffset\": 6",
#             " \"Score\": 0.9999970197767496": " \"Score\": 0.9999982118638471",
#             " \"Text\": \"Palmer\"": " \"Text\": \"Palmer\"",
#             " \"Type\": \"BRAND\"}]": " \"Type\": \"BRAND\"}]",
#             " \"File\": \"avs4.csv\"": " \"File\": \"avs4.csv\"",
#             " \"Line\": 157}": " \"Line\": 158}",
#             "": "",
#             "output": "output"
#         }
#     }
#     """
#     data = {}
#     pos = {}
#
#     # Open a csv reader called DictReader
#     with open(csvFilePath, encoding='utf-8') as csvf:
#         csvReader = csv.DictReader(csvf)
#
#         # Convert each row into a dictionary
#         # and add it to data
#         # k = 0
#         for rows in csvReader:
#             # k += 1
#             print(rows)
#             # Assuming a column named 'No' to
#             # be the primary key
#             try:
#                 # data[k] = rows
#                 keys = list(rows.keys())
#                 # print(keys)
#                 b = keys[0].split()[2].replace('"', '')
#                 e = keys[1].split()[1].replace('"', '')
#                 brand = keys[3].split()[1].replace('"', '')
#                 line = keys[6].split()[1].replace('"', '').replace('}', '')
#                 score = keys[2].split()[1].replace('"', '')
#                 pos[line] = [b, e, score, brand]
#             except KeyError as e:
#                 print(e)
#         print(pos)
#         # Open a json writer, and use the json.dumps()
#     # function to dump data
#     with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
#         jsonf.write(json.dumps(data, indent=4))
#
#     # Driver Code


# def read_json(jsonFilePath):
#     with open(jsonFilePath) as f:
#         data = json.load(f)
#     # print(data.keys())
#     # print(data['1'])
#     print(data['1'].keys())
#     print(type(data['1']))
#     # print(data['1'][0])
#     print(data['1'])
#     x = str(data['1'])
#     print(x)
#     y = json.loads(x)
#     print(y["Entities"])
#
#     # print(data['1']['Entities'])


def read_csv(csvFilePath):
    print(f'Input file: {csvFilePath}')
    with open(csvFilePath, 'r') as f:
        csv_content = f.readlines()
        csv_content = csv_content[2:]
        cols = csv_content[0].split(',')
        tfile = cols[5].split(':')[1].replace('"', '').replace(' ', '')
    with open(tfile, 'r') as f:
        lines = f.readlines()
    for i in csv_content:
        cols = i.split(',')
        try:
            b = int(cols[0].split(':')[2].replace('"', ''))
            e = int(cols[1].split(':')[1].replace('"', ''))
            # s = float(cols[2].split(':')[1].replace('"', ''))
            # brand = cols[3].split(':')[1].replace('"', '')
            # f = cols[5].split(':')[1].replace('"', '')
            m = int(cols[6].split(':')[1].replace('}"', ''))
            line = lines[m].rstrip()
            line = line.replace('"', '')
            chars = list(line)

            for n in range(b+1, e):
                chars[n] = '*'
            line = "".join(chars)
            print(m, line)
        except (ValueError, IndexError):
            pass

def main():
    # csvFilePath = "avs6a.csv"
# Function to convert a CSV to JSON
# Takes the file paths as arguments
# Decide the two file paths according to your
# computer system
    csvFilePath = r'avs6a.csv'
    # jsonFilePath = r'Names.json'

    # Call the make_json function
    # make_json(csvFilePath, jsonFilePath)
    # read_json(jsonFilePath)
    read_csv(csvFilePath)
main()
