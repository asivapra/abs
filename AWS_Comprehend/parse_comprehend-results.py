#!/usr/bin/env python

import csv
import json


# def parse(cols):
#     b = int(cols[0].split(':')[2].replace('"', ''))
#     e = int(cols[1].split(':')[1].replace('"', ''))
#     # s = float(cols[2].split(':')[1].replace('"', ''))
#     # br = cols[3].split(':')[1].replace('"', '')
#     # f = cols[5].split(':')[1].replace('"', '')
#     f = cols[6].split(':')
#     m = int(cols[6].split(':')[1].replace('}', '').replace('"', ''))
#     return b, e, m


# def mask_brands(b, e, m, lines):
#     line = lines[m].rstrip()
#     line = line.replace('"', '')
#     chars = list(line)
#
#     for n in range(b + 1, e):
#         chars[n] = '*'
#     line = "".join(chars)
#     return line


def mask_entities(offsets, doc_lines):
    l_no = list(offsets.keys())[0]
    # print(l_no)
    line = doc_lines[l_no].rstrip().replace('"', '')
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
    return line


def get_offsets(n_match, cols):
    offsets = {}
    if n_match == 1:
        b = int(cols[0].split(':')[2].replace('"', ''))  # BeginOffset
        e = int(cols[1].split(':')[1].replace('"', ''))  # EndOffset
        r = int(cols[6].split(':')[1].replace('}', '').replace('"', ''))  # Line == Row in document
        offsets[r] = [[b], [e]]

    elif n_match == 2:
        b1 = int(cols[0].split(':')[2].replace('"', ''))  # BeginOffset
        e1 = int(cols[1].split(':')[1].replace('"', ''))  # EndOffset
        b2 = int(cols[5].split(':')[1].replace('"', ''))  # BeginOffset
        e2 = int(cols[6].split(':')[1].replace('"', ''))  # EndOffset
        r = int(cols[11].split(':')[1].replace('}', '').replace('"', ''))  # Line == Row in document
        offsets[r] = [[b1, b2], [e1, e2]]

    elif n_match == 3:
        b1 = int(cols[0].split(':')[2].replace('"', ''))  # BeginOffset
        e1 = int(cols[1].split(':')[1].replace('"', ''))  # EndOffset
        b2 = int(cols[5].split(':')[1].replace('"', ''))  # BeginOffset
        e2 = int(cols[6].split(':')[1].replace('"', ''))  # EndOffset
        b3 = int(cols[10].split(':')[1].replace('"', ''))  # BeginOffset
        e3 = int(cols[11].split(':')[1].replace('"', ''))  # EndOffset
        r = int(cols[16].split(':')[1].replace('}', '').replace('"', ''))  # Line == Row in document
        offsets[r] = [[b1, b2, b3], [e1, e2, e3]]

    return offsets


def parse_cer_result(cer_content, doc_lines, masked_doc_file):
    k = 0
    with open(masked_doc_file, "a") as mf:
        for i in cer_content:
            k += 1
            # if k < 5700:
            #     continue
            cols = i.split(',')
            if not cols[3]:  # Discard empty rows, if any
                # print("A. Here:", i)
                continue
            # How many matches?
            if "Line" in cols[6]:
                n_match = 1
            elif "Line" in cols[11]:
                n_match = 2
            elif "Line" in cols[16]:
                n_match = 3
            else:
                n_match = 0  # This row has no line number
                # print("B. Here:", i)
                # return ""
            if n_match:
                offsets = get_offsets(n_match, cols)
                line = mask_entities(offsets, doc_lines) + "\n"
                mf.writelines(line)
    return k


def read_infile(doc_file):
    with open(doc_file, 'r') as f:
        lines = f.readlines()
    return lines


def get_infilename(cer_content):
    cols = cer_content[0].split(',')
    for i in range(0, len(cols)):
        v = cols[i]
        if "File" in v:
            tf = v.split(':')[1].replace('"', '').replace(' ', '')
            return tf


def read_cer(cer_file):
    print(f'Input file: {cer_file}')
    with open(cer_file, 'r') as f:
        cer_content = f.readlines()  # This is the CSV file saved from the Custom Entities Recognition
        cer_content = cer_content[2:]
    return cer_content


# def read_csv(cer_file):
#     print(f'Input file: {cer_file}')
# #col0	col1	col2	col3	col4	col5	col6	col7	col8	col9	col10	col11
# #{"Entities": [{"BeginOffset": 0	 "EndOffset": 6	 "Score": 0.9999930859092101	 "Text": "Palmer"	 "Type": "BRAND"}	 {"BeginOffset": 9	 "EndOffset": 16	 "Score": 0.9999995231630692	 "Text": "Natural"	 "Type": "BRAND"}]	 "File": "avs4.csv"	 "Line": 164}						output
#
#     with open(cer_file, 'r') as f:
#         cer_result = f.readlines()  # This is the CSV file saved from the Custom Entities Recognition
#         cer_result = cer_result[2:]
#         print(len(cer_result))
#         cols = cer_result[0].split(',')
#         for i in range(0, len(cols)):
#             v = cols[i]
#             # print(i)
#             if "File" in v:
#                 tf = v.split(':')[1].replace('"', '').replace(' ', '')
#                 print(tf)
#     with open(tf, 'r') as f:
#         lines = f.readlines()
#     k = 0
#     for i in cer_result:
#         k += 1
#         if k > 8:
#             break
#         # print(i)
#         cols2 = []
#         offsets = {}
#         cols = i.split(',')
#         if "File" not in cols[5]:
#             cols2_0 = "\"Entities\"\": " + cols[5]
#             cols2.append(cols2_0)
#             # print(cols2_0)
#             for n in range(6, 12):
#                 cols2.append(cols[n])
#             # break
#             cols[5] = cols[10]
#             cols[6] = cols[11]
#             # print("A. len cols2:", cols2)
#         try:
#             b, e, m = parse(cols)
#             offsets[m] = [[b], [e]]
#             # line = mask_brands(b, e, m, lines)
#             # print(m, line, len(cols2))
#             # print("B. len cols2:", len(cols2))
#             if len(cols2):
#                 b, e, m = parse(cols2)
#                 offsets[m][0].append(b)
#                 offsets[m][1].append(e)
#                 # line = mask_brands(b, e, m, lines)
#                 # print("from cols2:", m, line)
#             # print(offsets)
#             l_no = list(offsets.keys())[0]
#             # print(l_no)
#             line = lines[l_no].rstrip()
#             # print(line)
#             bs = offsets[l_no][0]
#             es = offsets[l_no][1]
#             chars = list(line)
#             for n in range(0, len(bs)):
#                 # print(bs[n], es[n])
#                 for n in range(bs[n] + 1, es[n]):
#                     chars[n] = '*'
#             line = "".join(chars)
#             print(l_no, line)
#         except (ValueError, IndexError) as e:
#             print(e)
#             pass


def main():
    """
    This program parses the results from "AWS Comprehend Custom Entities Recognition" and masks the
    entities in the original source document.

    Tested with "BrandsRecognizer" in AWS using 'avs4.csv' as source file.

    How it works:
        1. The 'BrandsRecognizer' is generated via AWS Comprehend as described in 'AWS Comprehend Custom Entities.docx'
            - Using 'brands.csv' as entities doc and 'avs4.csv' as training doc.
        2. The 'avs4.csv' is the same as 'amcal_products_u.txt'
        3. The 'avs6a.csv' is the output from the AWS Comprehend analysis.

        4. The content in 'avs6a.csv' is read and the top 2 lines removed to form 'cer_content'
            (CER = Custom Entities Recognition)
        5. The content in 'avs4.csv' is read as 'doc_lines'
        6. In parse_cer_result:
            - Each line in 'cer_content' can have up to 3 entities.
            - A doc_line with more than 3 entities will not have the line number in cer_content and cannot be used.
            - Each line is parsed to take the Begin/End offsets of the matching entities.
            - Each doc_line is now parsed to mask the entities using the offset values.
            - write out in 'masked_doc_file'
    See the comments in each function for more details.

    :return:
    """
    cer_file = r'avs6a.csv'  # This is the CSV file saved from the Custom Entities Recognition
    cer_content = read_cer(cer_file)  # This is the cer_file content
    doc_file = get_infilename(cer_content)  # This is the file containing the document lines to scan
    masked_doc_file = doc_file
    masked_doc_file = masked_doc_file.replace(".csv", "_masked.csv")
    doc_lines = read_infile(doc_file)
    k = parse_cer_result(cer_content, doc_lines, masked_doc_file)
    print(f"Processed {k} lines")


main()
