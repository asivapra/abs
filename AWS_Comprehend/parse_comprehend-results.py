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
import spacy
from spacy.pipeline import EntityRuler
import collections
from spellchecker import SpellChecker

cer_file = r'avs6b.csv'  # This is the CSV file saved from the AWS Custom Entities Recognition
doc_file = "avs4a.csv"
doc_file_masked = "avs4a_masked.csv"
analysis = 2  # 1 = analyse the results from the AWS Comprehend CER analysis. 2 = My own NLP method
cer_entities_file = './brand_names.txt'
spell = SpellChecker()
spell.word_frequency.load_text_file(cer_entities_file)

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

    :param cer_content: The AWS output data. See read_cer_file(cer_file) for details
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
        for ir in cer_content:
            offsets = {}
            ba = []
            ea = []

            # Replace the double double quotes around the keys
            # and around the commas separating the items. These are added when saving as CSV.

            # {""Entities"": [{""BeginOffset"": 0","  to {"Entities": [{"BeginOffset": 0",
            # ir = i.replace('""', '"').replace('","', ',')

            # ",,,,,,,,,,,output to '' and "{"Entities" to {"Entities"
            # ir = re.sub(r'",*output', '', ir).replace('"{"', '{"')

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


def parse_cer_result_0(cer_content, doc_lines, masked_doc_file):
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

    :param cer_content: The AWS output data. See read_cer_file(cer_file) for details
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


def read_doc_file(doc_file):
    """
    Read the file contents from the source document. e.g. 'avs4.csv'.
    :param doc_file: The filename returned by get_doc_file_names(cer_content)
    :return: lines == doc_lines. The full content as a list.
    """
    with open(doc_file, 'r') as f:
        lines = f.readlines()
    return lines


def get_doc_file_names(cer_content):
    """
    Get the filename of the source document. e.g. 'avs4.csv'. This filename is included
    in the AWS output data and is on every line except incomplete and more than 3 entities lines.

    :param cer_content: The AWS output data. See read_cer_file(cer_file) for details
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


def read_cer_file():
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


def lemmatise(text, nlp):
    # Implementing lemmatization
    lt = []

    # It is not necessary to lower case the words since the 'token.lemma_' will do it.
    # However, some proper nouns are not lower cased by the lemma_ function.
    text = text.lower()
    doc = nlp(text)
    for token in doc:
        # Remove stopwords (like the, an, of), punctuations and junk (like etc., i.e.)
        if not token.is_stop and not token.is_punct and not token.pos_ == 'X':
            # p = nltk.PorterStemmer()
            # word = p.stem(token.lemma_)
            # lt.append(word)  # Add the lemma of the word in an array
            # lt.append(token.lemma_)  # Add the lemma of the word in an array
            lt.append(token.text)  # Add the lemma of the word in an array
    return " ".join(lt)              # Return it as a full string


# def check_brands(d1, d2):
#     # print(d1, ":", d2)
#     doc1_ents = []
#     doc2_ents = []
#     for ent in d1.ents:
#         if ent.label_ == 'brand':
#             doc1_ents.append(ent.text)
#     for ent in d2.ents:
#         if ent.label_ == 'brand':
#             doc2_ents.append(ent.text)
#     diff_brands = 0
#     if len(doc1_ents) == 0 or len(doc2_ents) == 0:
#         return 0
#     else:
#         for brand in doc1_ents:
#             if brand not in doc2_ents:
#                 diff_brands = 1
#     if diff_brands:
#         return diff_brands
#     else:
#         return 0


def check_entities(keys, nlp):
    with open(doc_file_masked, "w") as mf:
        line = "Lines with brand names masked out\n"
        mf.writelines(line)
        keys = keys[0:10]
        n = 0
        for key in keys:
            n += 1
            print(n)
            doc_cw = []
            ii = []
            words = key.split()
            for w in words:
                if len(w) < 3:
                    doc_cw.append(w)
                elif re.search('\d', w):
                    doc_cw.append(w)
                else:
                    cw = spell.correction(w)
                    doc_cw.append(cw)
            doc1 = " ".join(doc_cw)
            doc1nlp = nlp(doc1)
            for ent in doc1nlp.ents:
                if ent.label_ == 'brand':
                    i = doc1.index(ent.text)
                    ii.append([i, len(ent.text)])
            s = list(doc1)
            for i in ii:
                k1 = i[0]
                k2 = i[1]
                for j in range(k1, k1+k2):
                    if s[j] == ' ':  # Do not change space to *
                        continue
                    s[j] = '*'
                line = key + "\t" + "".join(s) + "\n"
            # print(line)
            mf.writelines(line)


def build_entity_ruler(nlp):
    global brands
    rulerBrands = EntityRuler(nlp, 'LOWER', overwrite_ents=True)
    with open(cer_entities_file, "r") as f:
        filecontent = f.readlines()
    s1 = {i.strip() for i in filecontent}  # Add to a set to remove duplicates
    brands = [item.strip() for item in s1 if item != '\n']
    for f in brands:
        rulerBrands.add_patterns([{"label": "brand", "pattern": f}])
    rulerBrands.name = 'rulerBrands'
    nlp.add_pipe(rulerBrands)
    return nlp


def create_dict(csvfile):
    # d0 = collections.OrderedDict()
    keys = []
    with open(csvfile) as f:
        for name in f:
            name = name.strip()
            try:  # There are lines with just the URLs. These will give an error
                if name == "Name":
                    continue
                # name = name.replace("\"", "").replace("'", "")
                # d0[name] = ''
                keys.append(name)
            except ValueError as e:
                print(e)
                pass
    return keys


def read_dictionary():
    """
    Read the dictionary.
    The stop words are not read separately. They are part of the nlp model.
    :return: None
    """
    # model = 'en_core_web_sm'
    # model = 'en_core_web_md'
    # model = 'en_core_web_lg'
    model = 'en'  # Using 'en' instead of 'en_core_web_md', as the latter has many words without vector data. Check!
    print("Starting to read the model:", model)
    # nlp = spacy.cli.download("en")  # Run this for the first time on a new server.
    # nlp = spacy.cli.download("en_core_web_sm")  # Run this for the first time on a new server.
    # nlp = spacy.cli.download("en_core_web_md")  # Run this for the first time on a new server.
    # nlp = spacy.cli.download("en_core_web_lg")  # Run this for the first time on a new server.
    # Smaller models: en_core_web_md and en_core_web_sm
    nlp = spacy.load(model)  # Use this for subsequent runs
    # sr = stopwords.words('english')
    return nlp


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
    global doc_file
    if analysis == 1:
        cer_content = read_cer_file()  # This is the cer_file content (cer = Custom Entity Recognition)
        doc_file, masked_doc_file = get_doc_file_names(cer_content)  # This is the file containing the document lines to scan
        doc_lines = read_doc_file(doc_file)
        k, j = parse_cer_result(cer_content, doc_lines, masked_doc_file)
        print(f"Processed: {k} lines with entities masked. Excluding {j} incomplete lines.")

    elif analysis == 2:
        # Do a comparison with entity recognition using my own NLP method.
        # cer_content = read_cer_file()  # This is the cer_file content (cer = Custom Entity Recognition)
        # doc_file, masked_doc_file = get_doc_file_names(cer_content)  # This is the file containing the document lines to scan
        nlp = read_dictionary()
        keys = create_dict(doc_file)
        j = len(keys)
        print("Total Lines:", j)
        nlp = build_entity_ruler(nlp)
        check_entities(keys, nlp)


if __name__ == '__main__':
    main()
