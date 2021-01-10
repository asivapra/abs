#!/usr/bin/env python
"""
Project: ABS/Word2vec
Git: https://github.com/asivapra/abs/tree/main/Word2vec
Author: Arapaut V. Sivaprasad
Created: 08/01/2021
Last Modified: 08/01/2021
"""
import gzip
import gensim
import logging
import os
import nltk
import string
import re
import spacy

from gensim.models import Word2Vec, KeyedVectors
import RAKE
from spellchecker import SpellChecker
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
spell = SpellChecker()
# nltk.download('punkt')
# nltk.download('stopwords')




logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO)


def show_file_contents(input_file):
    with gzip.open(input_file, 'rb') as f:
        for i, line in enumerate(f):
            print(line)
            break


def read_input(input_file):
    """
    This method reads the input file which is in gzip format
    """

    logging.info("reading file {0}...this may take a while".format(input_file))
    with gzip.open(input_file, 'rb') as f:
        for i, line in enumerate(f):

            if i % 10000 == 0:
                logging.info("read {0} reviews".format(i))
            # do some pre-processing and return list of words for each review
            # text
            yield gensim.utils.simple_preprocess(line)


def build_save_model(docs):
    print("Building, training and saving the model...")
    model = gensim.models.Word2Vec(
        docs,
        size=150,
        window=10,
        min_count=2,
        workers=10)
    model.train(docs, total_examples=len(docs), epochs=10)

    # save only the word vectors
    # model.wv.save(os.path.join(abspath, "../vectors/default"))
    model.wv.save_word2vec_format('hotelreviews_model.bin', binary=True)
    return model


def read_train_save_model():
    abspath = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(abspath, "reviews_data.txt.gz")
    documents = list(read_input(data_file))
    logging.info("Done reading the data file. Now building and saving the model.")
    build_save_model(documents)


def Sort_Tuple(tup):
    tup.sort(key = lambda x: x[1])
    return tup


def read_lines():
    with open("reviews_data_1.txt", "r") as f:
        for i, line in enumerate(f):
            cols = line.split("\t")
            yield cols


def similar_words(model, rake_object):
    # stop_dir = "../AWS_Comprehend/Classification/stopwords.txt"
    # rake_object = RAKE.Rake(stop_dir)
    reviews = list(read_lines())
    print(len(reviews))
    for i in range(1):
        s = reviews[i]
        keywords = Sort_Tuple(rake_object.run(s))[:]
        print(len(keywords), s)
        for k in keywords:
            try:
                w1 = k[0]
                score = k[1]

                similar_words = []
                words = model.most_similar(positive=w1)
                for w in words:
                    spell_corrected_word = spell.correction(w[0])
                    similar_words.append(spell_corrected_word)
                similar_words = sorted(set(similar_words))
                print("{:s}: {:0.2f}".format(w1, score), similar_words)
            except Exception as e:
                # print(k)
                # print(f"*****{e}")
                pass


def most_common_phrases(rake_object, nlp):
    lines = list(read_lines())
    with open("results.txt", "w") as f:
        f.writelines(f"CS\tdiff\tdoc1\tdoc2\n")
        for i in range(1, 31):
            class1 = lines[i][0]
            translator = str.maketrans(string.punctuation, ' ' * len(string.punctuation))  # map punctuation to space. Works
            line1 = lines[i][1].translate(translator).strip()
            doc1 = lemmatise(line1, nlp)
            doc1nlp = nlp(doc1)
            print(i, line1)
            tot_csh = {}
            k = 0
            prev_class = ""
            tot_cs = 0.00
            for j in range(1, 31):
                k += 1
                class2 = lines[j][0]
                this_class = class2
                if not prev_class:
                    prev_class = this_class
                translator = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
                line2 = lines[j][1].translate(translator)
                key_phrases = Sort_Tuple(rake_object.run(line2))[:]  # returns [(key/phrase, score)...] in ascending score order
                substring = ""
                try:
                    for s in key_phrases[:]:  # Take the highest 10 phrases.
                        # print(s)
                        if s[1] > 1.0:
                            substring += s[0] + " "
                except Exception as e:
                    print(e)
                    pass
                doc2 = lemmatise(substring, nlp)
                doc2nlp = nlp(doc2)
                cs = round(doc1nlp.similarity(doc2nlp), 2)  # Average time: 1,993 microsec
                # print(i, j, cs, class1, class2)
                try:
                    tot_csh[this_class] += cs
                except:
                    tot_csh[this_class] = cs

                if prev_class == this_class:
                    tot_cs += cs
                else:
                    prev_class = ""
                    k = 0
                    # avg_cs = round(tot_cs / (k-1), 2)
                    # print(f"avgs_cs:{avg_cs} = {tot_cs} / {k-1}")
                    # tot_cs = 0.00
                    # f.writelines(f"Avg:{avg_cs}\t\t\n")
                f.writelines(f"{cs}\t{0.91-cs}\t{line1}\t{line2}\n")
            # print(class1, tot_csh)
            # avg_cs = round(tot_cs / (k - 1), 2)
            # print(f"avgs_cs:{avg_cs} = {tot_cs} / {k - 1}")
            # f.writelines(f"Avg:{avg_cs}\t\t\n")
            # print(i, j, class1, class2, cs, substring)


def most_common_words():
    reviews = list(read_lines())
    words = []
    for i in range(1):
        line = reviews[i].translate(str.maketrans('', '', string.punctuation))  # Remove punctuations

        print("line:", line)
        text_tokens = word_tokenize(line)
        tokens_without_sw = [word for word in text_tokens if word not in stopwords.words() and len(word) >= 4]
        # keywords = reviews[i].split()
        # print(s)
        # keywords = s.split()
        # keywords = Sort_Tuple(rake_object.run(s))[:]
        print("A.:", tokens_without_sw)
        for k in tokens_without_sw:
            words.append(k)
    print(words)
    C = Counter(words)
    most_occur = C.most_common(4)
    print(most_occur)


def get_rake_object():
    stop_dir = "../AWS_Comprehend/Classification/stopwords.txt"
    rake_object = RAKE.Rake(stop_dir)
    return rake_object


def read_dictionary():
    """
    Read the dictionary.
    The stop words are not read separately. They are part of the nlp model.
    :return: None
    """
    # model = 'en_core_web_sm'
    model = 'en_core_web_md'
    # model = 'en_core_web_lg'
    # model = 'en'  # Using 'en' instead of 'en_core_web_md', as the latter has many words without vector data. Check!
    print("Starting to read the model:", model)
    # nlp = spacy.cli.download(model)  # Run this for the first time on a new server.
    nlp = spacy.load(model)  # Use this for subsequent runs
    return nlp


def lemmatise(text, nlp):
    # Implementing lemmatization
    lt = []
    # It is not necessary to lower case the words since the 'token.lemma_' will do it.
    # However, some proper nouns are not lower cased by the lemma_ function.
    text = text.lower()
    doc = nlp(text)
    for token in doc:
        # Remove stopwords (like the, an, of), punctuations and junk (like etc., i.e.)
        # if not token.is_stop and not token.is_punct and not token.pos_ == 'X':
        if not token.is_stop and not token.is_punct:
            lt.append(token.text)  # Add the lemma of the word in an array
    return " ".join(lt)              # Return it as a full string


def main():
    # For reading and training. Required once only
    # read_train_save_model()  # Comment this out in subsequent runs
    nlp = read_dictionary()

    rake_object = get_rake_object()

    model = KeyedVectors.load_word2vec_format('hotelreviews_model.bin', binary=True)
    # lookup(model)
    # similar_words(model, rake_object)
    # most_common_words()
    most_common_phrases(rake_object, nlp)


def lookup(model):
    w1 = ["dirty"]
    print("Most similar to {0}".format(w1), model.most_similar(positive=w1))

    w1 = ["dirty"]
    print("Most dissimilar to {0}".format(w1), model.most_similar(negative=w1))

    # look up top 6 words similar to 'polite'
    w1 = ["polite"]
    print("Most similar to {0}".format(w1), model.most_similar(positive=w1, topn=6))

    w1 = ["polite"]
    print("Most dissimilar to {0}".format(w1), model.most_similar(negative=w1, topn=6))

    # look up top 6 words similar to 'france'
    w1 = ["france"]
    print(
        "Most similar to {0}".format(w1),
        model.most_similar(
            positive=w1,
            topn=6))

    # look up top 6 words similar to 'shocked'
    w1 = ["shocked"]
    print(
        "Most similar to {0}".format(w1),
        model.most_similar(
            positive=w1,
            topn=6))

    # look up top 6 words similar to 'shocked'
    w1 = ["beautiful"]
    print(
        "Most similar to {0}".format(w1),
        model.most_similar(
            positive=w1,
            topn=6))

    # get everything related to stuff on the bed
    w1 = ["bed", 'sheet', 'pillow']
    w2 = ['blanket']
    print(
        "Most similar to {0}".format(w1),
        model.most_similar(
            positive=w1,
            negative=w2,
            topn=10))

    # similarity between two different words
    print("Similarity between 'dirty' and 'smelly'",
          model.similarity(w1="dirty", w2="smelly"))

    # similarity between two identical words
    print("Similarity between 'dirty' and 'dirty'",
          model.similarity(w1="dirty", w2="dirty"))

    # similarity between two unrelated words
    print("Similarity between 'dirty' and 'unclean'",
          model.similarity(w1="dirty", w2="unclean"))


if __name__ == '__main__':
    main()

