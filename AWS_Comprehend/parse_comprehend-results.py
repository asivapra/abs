#!/usr/bin/env python
"""
parse_comprehend-results.py
GitHub: https://github.com/asivapra/abs/blob/main/AWS_Comprehend/parse_comprehend-results.py
    This program parses the results from "AWS Comprehend Custom Entities Recognition" and masks the
    entities in the original source document.

Author: Dr. Arapaut V. Sivaprasad
Created on: 28-12-2020
Last Modified on: 29-12-2020
Copyright (c) 2020 by Arapaut V. Sivaprasad, Australian Bureau of Statistics and WebGenie Software Pty Ltd.
"""

import re
import json

cer_file = r'avs6a.csv'  # This is the CSV file saved from the AWS Custom Entities Recognition


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


def parse_cer_result(cer_content, doc_lines, masked_doc_file):
    """
    This function cleanses the output from the AWS Athena query of the database table. Then, the required
    offsets for the entities in the lines are taken and used to mask the entities.

    1. Parse the AWS output data to get the Begin/End offsets and line number for the entities.

        - Original lines in the AWS table search:
        {"Entities": [{"BeginOffset": 0	 "EndOffset": 6	 "Score": 0.9999970197767496	 "Text": "Palmer"
        "Type": "BRAND"}]	 "File": "avs4.csv"	 "Line": 157}

        - When saved as CSV, extra double quotes are added.
        - Also, depending on the number of entities on a line, there will be extra commas and the word, 'output'
        - These must be removed before parsing.
            "{""Entities"": [{""BeginOffset"": 0"," ""EndOffset"": 10"," 
            ""Score"": 0.9999819997552802"," ""Text"": ""GO Healthy""\","
            ""Type"": ""BRAND""}]"," ""File"": ""avs4.csv""\"," ""Line"": 5663}",,,,,,,,,,,output

        - In this function the lines are converted into a string as below, which is the same as in the AWS table.
        - Then it can be JSON loaded into a dictionary object.
            {"Entities" : [{"BeginOffset":int, "EndOffset":int, "Score": float, "Text": str, "Type": str}],
            "File": str, "Line": int}

    2. Each line will give the BeginOffset, EndOffset and Row (b, e, r). No need to take the entity's 'Text'

    3. Using the b, e and r, mask the entities in the original doc_lines and write out in a file.

    :param cer_content: The AWS output data. See read_cer(cer_file) for details
    :param doc_lines: The full content of the original document as a list.
    :param masked_doc_file: Output filename to write the masked lines.
    :return: k, j == The numbers of lines actually parsed and the excluded lines
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

            # Replace the double double quotes around the keys
            # and around the commas separating the items. These are added when saving as CSV.

            # {""Entities"": [{""BeginOffset"": 0","  to {"Entities": [{"BeginOffset": 0",
            ir = i.replace('""', '"').replace('","', ',')

            # ",,,,,,,,,,,output to '' and "{"Entities" to {"Entities"
            ir = re.sub(r'",*output', '', ir).replace('"{"', '{"')

            try:
                # The following line will give an error if the line does not end with a }.
                #    - ERROR: Expecting ',' delimiter: line 2 column 1
                # It happens when there are more than three entities in the line as below.
                # {"Entities": [{"BeginOffset": 0	 "EndOffset": 5	 "Score": 0.9999775891557117
                # "Text": "Napro"	 "Type": "BRAND"}	 {"BeginOffset": 6	 "EndOffset": 13
                # "Score": 0.9999967813595916	 "Text": "Palette"	 "Type": "BRAND"}	 {"BeginOffset": 14
                # "EndOffset": 18	 "Score": 0.9999977350285647	 "Text": "Hair"	 "Type": "BRAND"}
                # {"BeginOffset": 19	 "EndOffset": 25
                dict_ir = json.loads(ir)  # 'dict_ir' is a dict object of the string, 'ir'

                r = dict_ir['Line']
                le = len(dict_ir['Entities'])
                if not le:  # There is no entity in the line.
                    j += 1
                for n in range(0, le):
                    ba.append(dict_ir['Entities'][n]['BeginOffset'])
                    ea.append(dict_ir['Entities'][n]['EndOffset'])
                offsets[r] = [ba, ea]
                line = mask_entities(offsets, doc_lines) + "\n"
                mf.writelines(line)
                k += 1
            except json.decoder.JSONDecodeError as e:
                j += 1
                print(f">>> ERROR: {e}. Skipping the line.")
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
    in the AWS output data and is on every line except incomplete and more than 3 entities lines.

    :param cer_content: The AWS output data. See read_cer(cer_file) for details
    :return: doc_file, masked_doc_file == the document file name and the output file to write the masked lines.
    """
    for i in (0, len(cer_content)):
        ir = cer_content[i].replace('""', '"').replace('","', ',')
        ir = re.sub(r'",*output', '', ir).replace('"{"', '{"')
        try:
            dict_ir = json.loads(ir)  # 'dict_ir' is a dict object of the string, 'ir'
            doc_file = dict_ir['File']
            masked_doc_file = doc_file.replace(".csv", "_masked.csv")
            return doc_file, masked_doc_file
        except json.decoder.JSONDecodeError as e:
            print(f">>> ERROR: {e}. Skipping the line.")


# def get_infilename_0(cer_content):
#     """
#     Get the filename of the source document. e.g. 'avs4.csv'. This filename is included
#     in the AWS output data. It is taken from the second last non-empty column in 'cer_content'
#
#     :param cer_content: The AWS output data. See read_cer(cer_file) for details
#     :return: tf == the document file name.
#     """
#     cols = cer_content[0].split(',')
#     for i in range(0, len(cols)):
#         v = cols[i]
#         if "File" in v:
#             tf = v.split(':')[1].replace('"', '').replace(' ', '')
#             return tf


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
    # cer_file = r'avs6a.csv'  # This is the CSV file saved from the Custom Entities Recognition
    cer_content = read_cer(cer_file)  # This is the cer_file content
    doc_file, masked_doc_file = get_infilename(cer_content)  # This is the file containing the document lines to scan
    # masked_doc_file = doc_file
    # masked_doc_file = masked_doc_file.replace(".csv", "_masked.csv")
    doc_lines = read_infile(doc_file)
    k, j = parse_cer_result(cer_content, doc_lines, masked_doc_file)
    print(f"Processed: {k} lines with entities masked. Excluding {j} incomplete lines.")


if __name__ == '__main__':
    main()
