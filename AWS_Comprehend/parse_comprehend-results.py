#!/usr/bin/env python

import csv
import json


def parse(cols):
    # print("I:", cols)
    b = int(cols[0].split(':')[2].replace('"', ''))
    # print("H:", cols[1])
    e = int(cols[1].split(':')[1].replace('"', ''))
    # print("G:", cols[6])
    # s = float(cols[2].split(':')[1].replace('"', ''))
    # br = cols[3].split(':')[1].replace('"', '')
    # f = cols[5].split(':')[1].replace('"', '')
    f = cols[6].split(':')
    # print("F:", f)
    m = int(cols[6].split(':')[1].replace('}', '').replace('"', ''))
    return b, e, m


def mask_brands(b, e, m, lines):
    line = lines[m].rstrip()
    line = line.replace('"', '')
    chars = list(line)

    for n in range(b + 1, e):
        chars[n] = '*'
    line = "".join(chars)
    return line


def read_csv(csvFilePath):
    print(f'Input file: {csvFilePath}')
#col0	col1	col2	col3	col4	col5	col6	col7	col8	col9	col10	col11
#{"Entities": [{"BeginOffset": 0	 "EndOffset": 6	 "Score": 0.9999930859092101	 "Text": "Palmer"	 "Type": "BRAND"}	 {"BeginOffset": 9	 "EndOffset": 16	 "Score": 0.9999995231630692	 "Text": "Natural"	 "Type": "BRAND"}]	 "File": "avs4.csv"	 "Line": 164}						output

    with open(csvFilePath, 'r') as f:
        csv_content = f.readlines()
        csv_content = csv_content[2:]
        print(len(csv_content))
        cols = csv_content[0].split(',')
        for i in range(0, len(cols)):
            v = cols[i]
            # print(i)
            if "File" in v:
                tf = v.split(':')[1].replace('"', '').replace(' ', '')
                print(tf)
    with open(tf, 'r') as f:
        lines = f.readlines()
    k = 0
    for i in csv_content:
        k += 1
        if k > 8:
            break
        # print(i)
        cols2 = []
        offsets = {}
        cols = i.split(',')
        if "File" not in cols[5]:
            cols2_0 = "\"Entities\"\": " + cols[5]
            cols2.append(cols2_0)
            # print(cols2_0)
            for n in range(6, 12):
                cols2.append(cols[n])
            # break
            cols[5] = cols[10]
            cols[6] = cols[11]
            # print("A. len cols2:", cols2)
        try:
            b, e, m = parse(cols)
            offsets[m] = [[b], [e]]
            # line = mask_brands(b, e, m, lines)
            # print(m, line, len(cols2))
            # print("B. len cols2:", len(cols2))
            if len(cols2):
                b, e, m = parse(cols2)
                offsets[m][0].append(b)
                offsets[m][1].append(e)
                # line = mask_brands(b, e, m, lines)
                # print("from cols2:", m, line)
            # print(offsets)
            l_no = list(offsets.keys())[0]
            # print(l_no)
            line = lines[l_no].rstrip()
            # print(line)
            bs = offsets[l_no][0]
            es = offsets[l_no][1]
            chars = list(line)
            for n in range(0, len(bs)):
                # print(bs[n], es[n])
                for n in range(bs[n] + 1, es[n]):
                    chars[n] = '*'
            line = "".join(chars)
            print(l_no, line)
        except (ValueError, IndexError) as e:
            print(e)
            pass

def main():
    csvFilePath = r'avs6a.csv'
    read_csv(csvFilePath)
main()
