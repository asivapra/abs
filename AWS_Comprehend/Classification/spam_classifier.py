#!/usr/bin/env python
"""
parse_comprehend-results.py
GitHub: https://github.com/asivapra/abs/blob/main/AWS_Comprehend/parse_comprehend-results.py
    This program parses the results from "AWS Comprehend Custom Entities Recognition" and masks the
    entities in the original source document.

Author: Dr. Arapaut V. Sivaprasad
Created on: 28-12-2020
Last Modified on: 01-01-2021
Copyright (c) 2020 by Arapaut V. Sivaprasad, Australian Bureau of Statistics and WebGenie Software Pty Ltd.
"""

import re
import json
import spacy
from spacy.pipeline import EntityRuler
from spellchecker import SpellChecker
import time
import multiprocessing as mp
 
spam_phrases = r'spam_phrases.txt'  # This is the CSV file saved from the AWS Custom Entities Recognition
doc_file = "spam_lines.txt"
doc_file_masked = "spam_lines_masked.txt"
analysis = 2  # 1 = analyse the results from the AWS Comprehend CER analysis. 2 = My own NLP method
limit = 0  # Limit the number of lines to be tested. Make this 0 for no limit.
spell = SpellChecker()
spell.word_frequency.load_text_file(spam_phrases)


def star_entities(b, e, d1, nlp, wr):
    """
    This function replaces the identified brands in each line with *'s.
    More than one brand in a line can be processed.
    How?
        - Each line is split into words
        - Skip words that are less than 4 letters. Hint: Profanity words are always 4 letters or more.
            - This means brands like 3M, A2, Aim, etc. will be skipped.
        - Skip words with a digit in it. This is to skip words like 50mg, 100u, etc.
            - It too will miss brands like A2, 3M.
        - Put the words through 'spell.correction'
            - It will correct spelling in most dictionary words.
            - Brands are generally not dictionary words, but they will be checked against the 'brand_names' since this
            file has been added via 'def build_entity_ruler'. It picks up most common misspellings.
        - Join the words to form a spelling corrected line which replaces the original line.
        - Create an NLP object, 'docnlp', with the new line.
        - Iterate through the 'docnlp.ent' objects and pick those which have the label, 'brand'.
        - Find the index position of this entity in the string, 'doc'.
        - Add the index and the length of this entity in a 2D array as 'ii = [[i, j]]'
        - Split the doc line into a list ('s').
        - Iterate through 's' and mask the words using their start index and length from 'ii'
        - Join 's' into a line and write it out.
    :param b: Start of lines to to be processed
    :param e: End of lines.
    :param keys: The brands' list as in brand_names.txt
    :param nlp: The NLP object using model 'en'
    :param wr: The worker number
    :return: None
    """
    if wr == 0:  # Means that the job is in serial mode.
        with open(doc_file_masked, "w") as mf:
            line = "Lines with brand names masked out\n"
            mf.writelines(line)

    with open(doc_file_masked, "a") as mf:
        if limit:
            e = b + limit  # Debugging. DO NOT DELETE. To limit the # of lines.
        print(f"Worker {wr}: Processing lines {b} to {e}.")
        keys = list(d1.keys())
        keys = keys[b:e]
        n = b
        for urk in keys:
            n += 1
            if wr == 0:
                print(f"w:{wr} :{n}")
            doc_cw = []  # Corrected words
            ii = []
            text = d1[urk]
            words = text.split()
            for w in words:
                if len(w) <= 3:
                    doc_cw.append(w)
                elif re.search('\d', w):
                    doc_cw.append(w)
                else:
                    cw = spell.correction(w)
                    doc_cw.append(cw)
            doc = " ".join(doc_cw)
            docnlp = nlp(doc)
            words = []
            # d2 = {}
            for ent in docnlp.ents:
                if ent.label_ == 'brand':
                    i = doc.index(ent.text)
                    ii.append([i, len(ent.text)])
                    words.append(ent.text.lower())
            # d2[urk] = words
            if len(words):
                s = list(doc)
                for i in ii:
                    k1 = i[0]
                    k2 = i[1]
                    for j in range(k1 + 1, k1 + k2):
                        if s[j] == ' ':  # Do not change space to *
                            continue
                        s[j] = '*'
                doc = "".join(s)
                my_set = set(words)
                text = ", ".join(my_set)
                print(urk, doc)
                mf.writelines(f"{urk}\tSpam\t{text}\t{doc}\n")
            else:
                mf.writelines(f"{urk}\t\t\t{doc}\n")
    print(f"Worker {wr}: Finished.")


def par_star_entities(d1, nlp):
    """
    Run it in parallel mode. This is to maximise performance
    :return: None
    """
    print("Warning: In Parallel Mode:")
    print("Writing masked lines in:", doc_file_masked)
    with open(doc_file_masked, 'w') as f:
        f.write("Original Lines\tMasked Lines\n")
    ct0 = time.perf_counter()
    keys = d1.keys()
    j = len(keys)
    print("Total Lines:", j)
    num_workers = mp.cpu_count() * 1

    chunk_size = j // num_workers
    n_chunks = j // chunk_size
    remainder = j - (chunk_size * num_workers)
    print("Starting: num_workers, chunk_size, remainder:", num_workers, chunk_size, remainder)
    workers = []
    # Each worker gets a subset of the URLs.
    # e.g. 16 workers and nn URLs:
    e = r = w = 0  # Declare these to avoid a warning
    for w in range(n_chunks):
        b = w * chunk_size
        e = b + chunk_size
        workers.append(mp.Process(target=star_entities, args=(b, e, d1, nlp, w)))

    # If the number of items is not an exact multiple of 'num_workers' there will be some leftover.
    # Start a new worker to handle those.
    # Below, j == the number of keys1. This is to be split across the workers.
    # e = total lines allocated across num_workers. If it is not the same as j, there is
    # some leftover. Start a new worker for it.
    try:
        if e:
            r = j - e
        if r > 0:  # See if any leftover
            w += 1
            workers.append(mp.Process(target=star_entities, args=(e, j, d1, nlp, w)))

    except Exception as e:
        print(e)
        pass
    with open(doc_file_masked, "w") as mf:
        line = "Lines with brand names masked out\n"
        mf.writelines(line)

    for w in workers:
        w.start()
    # Wait for all processes to finish
    for w in workers:
        w.join()
    et = time.perf_counter() - ct0
    print("Finished. Time: {:0.2f} sec".format(et))


def build_entity_ruler(nlp):
    """
    Add the brand names as custom entities to the NLP object.
    :param nlp: The NLP object using the model, 'en'
    :return: nlp = the modified NLP object
    """
    rulerBrands = EntityRuler(nlp, 'LOWER', overwrite_ents=True)
    with open(spam_phrases, "r") as f:
        filecontent = f.readlines()
    s1 = {i.strip() for i in filecontent}  # Add to a set to remove duplicates
    brands = [item.strip() for item in s1 if item != '\n']
    for brand_name in brands:
        rulerBrands.add_patterns([{"label": "brand", "pattern": brand_name}])
    rulerBrands.name = 'rulerBrands'
    nlp.add_pipe(rulerBrands)
    return nlp


def read_spam_file(tsvfile):
    """
    Read the input file and return its content in a list.
    :param tsvfile: Input file
        The tsvfile contains the original text lines as:
            Name
            Leukopot Snap Spool 1.25cm x 5m
    :return: keys = list of lines in the input file.
    """
    lines = {}
    with open(tsvfile) as f:
        for line in f:
            try:
                line = line.strip().split("\t")
                urk = line[0]
                text = line[1]
                try:  # There are lines with just the URLs. These will give an error
                    if line[0] == "Name":
                        continue
                    lines[urk] = text
                except ValueError as e:
                    pass
            except IndexError as e:
                pass
    # print(lines['98510812438'])
    return lines


def read_dictionary():
    """
    Read the dictionary.
    The stop words are not read separately. They are part of the nlp model.
    :return: None
    """
    model = 'en'  # Using 'en' instead of 'en_core_web_md', as the latter has many words without vector data. Check!
    print("Starting to read the model:", model)
    # nlp = spacy.cli.download("en")  # Run this for the first time on a new server.
    nlp = spacy.load(model)  # Use this for subsequent runs
    return nlp


def main():
    """
    This program parses the results from "AWS Comprehend Custom Entities Recognition" and masks the
    entities in the original source document.

    Tested with "BrandsRecognizer" in AWS using 'avs4.csv' as source file.

    How it works:
        This does not use the AWS Comprehend output at all. Instead, uses a custom entity file (e.g. brand_names.txt')
        and the document input file to directly compare and mask the words/phrases.

        This method is devised as an alternative to AWS Comprehend which has a potential problem of not detecting
        all occurrences of the words/phrases in the doc lines. See the doc comment in 'def star_entities' for details.

    See the comments in each function for more details.

    :return:
    """
    global doc_file
    lines = []
    # Do a comparison with entity recognition using my own NLP method.
    nlp = read_dictionary()
    d1 = read_spam_file(doc_file)  # This is the doc with the spam lines.
    keys = d1.keys()
    j = len(keys)
    print("Total Lines:", j)
    for urk in keys:
        text = d1[urk]
        lines.append(text)
        # print(f"URK: {urk} - Line: {text}")
    nlp = build_entity_ruler(nlp)
    par_star_entities(d1, nlp)              # Run it as a parallel job.


if __name__ == '__main__':
    main()
