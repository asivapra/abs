#!/usr/bin/env python
"""
parse_comprehend-results.py
GitHub: https://github.com/asivapra/abs/blob/main/AWS_Comprehend/parse_comprehend-results.py
    This program parses the results from "AWS Comprehend Custom Entities Recognition" and masks the
    entities in the original source document.

Author: Dr. Arapaut V. Sivaprasad
Created on: 28-12-2020
Last Modified on: 28-12-2020
Copyright (c) 2020 by Arapaut V. Sivaprasad, Australian Bureau of Statistics and WebGenie Software Pty Ltd.
"""

import re
import json

def mask_entities(offsets, doc_lines):
    """
    Mask the entities in the original source document and write out.
    :param offsets: The Begin/End positions for the entities. Received as a dict with the line number as the key.
    :param doc_lines: The full content of the original document as a list.
    :return: line = The entities masked out line.
    """
    line_no = list(offsets.keys())[0]
    # print(line_no)
    line = doc_lines[line_no].rstrip().replace('"', '')
    bs = offsets[line_no][0]
    es = offsets[line_no][1]
    chars = list(line)
    for m in range(0, len(bs)):
        for n in range(bs[m] + 1, es[m]):
            chars[n] = '*'
    line = "".join(chars)
    # print(line_no, line)  # For debugging. Do NOT delete
    return line


def get_offsets(cols):
# {"Entities": [{"BeginOffset": 0,	 "EndOffset": 6,	 "Score": 0.9999930859092101,	 "Text": "Palmer",	 "Type": "BRAND"},	 {"BeginOffset": 9,	 "EndOffset": 16,	 "Score": 0.9999995231630692,
# "Text": "Natural",	 "Type": "BRAND"}],	 "File": "avs4.csv",	 "Line": 164}

    offsets = {}
    ba = []
    ea = []
    r = 0
    n_match = 0
    n_excluded = 0
    # Get the line number
    for i in cols:
        if "Line" in i:
            r = int(i.split(':')[1].replace('}', '').replace('"', '').strip())
            break
    if r:
        for j in range(0, len(cols)):
            if "BeginOffset" in cols[j]:
                if j == 0:
                    b = int(cols[j].split(':')[2].replace('"', ''))  # BeginOffset
                else:
                    b = int(cols[j].split(':')[1].replace('"', ''))  # BeginOffset
                ba.append(b)
                n_match += 1

            if "EndOffset" in cols[j]:
                e = int(cols[j].split(':')[1].replace('"', ''))  # EndOffset
                ea.append(e)
    else:
        n_excluded = 1

    offsets[r] = [ba, ea]
    if not n_match:
        pass
        # print(offsets, n_match, n_excluded)
    return offsets, n_match, n_excluded


def get_offsets_0(n_match, cols):
    """
    Get the offsets of entities in the source document lines. This info is retrieved from AWS output data.

    :param n_match: Number of entities in a line
    :param cols: The results line split as columns
    :return: offsets == a dict of offsets using the line number as keys. The values are arrays of
    Begin/End offsets. There can be up to 3 values for these.
        e.g.    {157 : [[0], [6]]}
                {157 : [[0, 10], [6, 15]]}
                {157 : [[0, 10, 17], [6, 15, 23]]}
    """
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
    """
    1. Parse the AWS output data to get the Begin/End offsets and line number for the entities.
    2. Mask the entities in the original doc_lines and write out in a file.
    :param cer_content: The AWS output data. See read_cer(cer_file) for details
    :param doc_lines: The full content of the original document as a list.
    :param masked_doc_file: Output filename to write the masked lines.
    :return: k == The number of lines actually parsed, excluding the lines skipped due the absence of line number
    """
    print(f"Output file: {masked_doc_file}")
    k = 0   # Number of lines where the entities are masked
    j = 0   # Number of lines skipped. These include those without an entity and those with > 3 entities.
    with open(masked_doc_file, "w") as mf:
        line = "Titles with Masked Entities\n"
        mf.writelines(line)
        for i in cer_content:
            offsets = {}
            ba = []
            ea = []
            ij = i.replace('""', '"').replace('","', ',')
            ij = re.sub(r'",*output', '', ij).replace('"{"', '{"')
            try:
                dd = json.loads(ij)
                r = dd['Line']
                le = len(dd['Entities'])
                if not le:  # There is no entity in the line.
                    j += 1
                for n in range(0, le):
                    ba.append(dd['Entities'][n]['BeginOffset'])
                    ea.append(dd['Entities'][n]['EndOffset'])
                offsets[r] = [ba, ea]
                line = mask_entities(offsets, doc_lines) + "\n"
                mf.writelines(line)
                k += 1
            except Exception as e:
                j += 1
                # print(">>> ERROR:", e)
    return k, j


def parse_cer_result_0(cer_content, doc_lines, masked_doc_file):
    """
    1. Parse the AWS output data to get the Begin/End offsets and line number for the entities.
    2. Mask the entities in the original doc_lines and write out in a file.
    :param cer_content: The AWS output data. See read_cer(cer_file) for details
    :param doc_lines: The full content of the original document as a list.
    :param masked_doc_file: Output filename to write the masked lines.
    :return: k == The number of lines actually parsed, excluding the lines skipped due the absence of line number
    """
    print(f"Output file: {masked_doc_file}")
    k = 0   # Number of lines masked
    j = 0  # Number of lines excluded
    with open(masked_doc_file, "a") as mf:
        for i in cer_content:
            k += 1
            # if k < 5700:  # For debugging. Do NOT delete
            #     continue
            cols = i.split(',')
            if not cols[3]:  # Discard empty rows, if any
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
                k -= 1
                j += 1
            if n_match:
                offsets = get_offsets(n_match, cols)
                line = mask_entities(offsets, doc_lines) + "\n"
                mf.writelines(line)
    return k, j


def read_infile(doc_file):
    """
    Read teh file contents from the source document. e.g. 'avs4.csv'.
    :param doc_file: The filename returned by get_infilename(cer_content)
    :return: lines == doc_lines. The full content as a list.
    """
    with open(doc_file, 'r') as f:
        lines = f.readlines()
    return lines


def get_infilename(cer_content):
    """
    Get the filename of the source document. e.g. 'avs4.csv'. This filename is included
    in the AWS output data. It is taken from the second last non-empty column in 'cer_content'

    :param cer_content: The AWS output data. See read_cer(cer_file) for details
    :return: tf == the document file name.
    """
    cols = cer_content[0].split(',')
    for i in range(0, len(cols)):
        v = cols[i]
        if "File" in v:
            tf = v.split(':')[1].replace('"', '').replace(' ', '')
            return tf


def read_cer(cer_file):
    """
    Read the content of the output from the AWS Comprehend analysis
    :param cer_file: The data input file. Contents as below.
        - The 'avs6a.csv' is the output from the AWS Comprehend analysis.

        Sample lines:
        1-Entity:
        {"Entities": [{"BeginOffset": 0,	 "EndOffset": 6,	 "Score": 0.9999970197767496,
        "Text": "Palmer",	 "Type": "BRAND"}],	 "File": "avs4.csv",	 "Line": 157}

        2-Entitites:
        {"Entities": [{"BeginOffset": 0,	 "EndOffset": 6,	 "Score": 0.9999930859092101,
        "Text": "Palmer",	 "Type": "BRAND"},	 {"BeginOffset": 9,	 "EndOffset": 16,
        "Score": 0.9999995231630692,	 "Text": "Natural",	 "Type": "BRAND"}],	 "File": "avs4.csv",
        "Line": 164}

        3-Entitites:
        {"Entities": [{"BeginOffset": 0,	 "EndOffset": 7,	 "Score": 0.9999976158197796,
        "Text": "Johnson",	 "Type": "BRAND"},	 {"BeginOffset": 10,	 "EndOffset": 14,
        "Score": 0.9999986886995842,	 "Text": "Head",	 "Type": "BRAND"},	 {"BeginOffset": 22,
        "EndOffset": 26,	 "Score": 0.9999995231630692,	 "Text": "Baby",	 "Type": "BRAND"}],
        "File": "avs4.csv"	 "Line": 5702}

    :return: cer_content
    """
    print(f'Input file: {cer_file}')
    with open(cer_file, 'r') as f:
        cer_content = f.readlines()  # This is the CSV file saved from the Custom Entities Recognition
        cer_content = cer_content[2:]
    return cer_content


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
    k, j = parse_cer_result(cer_content, doc_lines, masked_doc_file)
    print(f"Processed: {k} lines with entities masked. Excluding {j} incomplete lines.")


if __name__ == '__main__':
    main()
