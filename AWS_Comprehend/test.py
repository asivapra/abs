#!/usr/bin/env python
#!/usr/bin/env python
# coding: utf8
"""Example of training spaCy's named entity recognizer, starting off with an
existing model or a blank model.

For more details, see the documentation:
* Training: https://spacy.io/usage/training
* NER: https://spacy.io/usage/linguistic-features#named-entities

Compatible with: spaCy v2.0.0+
Last tested with: v2.2.4
"""
from __future__ import unicode_literals, print_function

import plac
import re
import random
import warnings
from pathlib import Path
import spacy
from spacy.util import minibatch, compounding
from spellchecker import SpellChecker
spell = SpellChecker()
spell.word_frequency.load_text_file('./brand_names.txt')

# training data
TRAIN_DATA = [
    ("Leukopor Hypoallergenic Paper Tape Snap Spool", {"entities": [(0, 8, "BRAND"), (9, 23, "BRAND")]}),
    ("Who is Shaka Khan?", {"entities": [(7, 12, "PERSON")]}),
    ("I like London and Berlin.", {"entities": [(7, 13, "LOC"), (18, 24, "LOC")]}),
]


@plac.annotations(
    model=("Model name. Defaults to blank 'en' model.", "option", "m", str),
    output_dir=("Optional output directory", "option", "o", Path),
    n_iter=("Number of training iterations", "option", "n", int),
)

def train_spacy(model=None, output_dir=None, n_iter=100):
    """Load the model, set up the pipeline and train the entity recognizer."""
    if model is not None:
        nlp = spacy.load(model)  # load existing spaCy model
        print("Loaded model '%s'" % model)
    else:
        nlp = spacy.blank("en")  # create blank Language class
        # nlp = spacy.load("en")  # create blank Language class
        print("Created blank 'en' model")

    # create the built-in pipeline components and add them to the pipeline
    # nlp.create_pipe works for built-ins that are registered with spaCy
    if "ner" not in nlp.pipe_names:
        ner = nlp.create_pipe("ner")
        nlp.add_pipe(ner, last=True)
    # otherwise, get it so we can add labels
    else:
        ner = nlp.get_pipe("ner")

    # add labels
    for _, annotations in TRAIN_DATA:
        for ent in annotations.get("entities"):
            ner.add_label(ent[2])

    # get names of other pipes to disable them during training
    pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
    # only train NER
    with nlp.disable_pipes(*other_pipes), warnings.catch_warnings():
        # show warnings for misaligned entity spans once
        warnings.filterwarnings("once", category=UserWarning, module='spacy')

        # reset and initialize the weights randomly – but only if we're
        # training a new model
        if model is None:
            nlp.begin_training()
        for itn in range(n_iter):
            # random.shuffle(TRAIN_DATA)
            losses = {}
            # batch up the examples using spaCy's minibatch
            batches = minibatch(TRAIN_DATA, size=compounding(4.0, 32.0, 1.001))
            for batch in batches:
                texts, annotations = zip(*batch)
                nlp.update(
                    texts,  # batch of texts
                    annotations,  # batch of annotations
                    drop=0.5,  # dropout - make it harder to memorise data
                    losses=losses,
                )
            # print("Losses", losses)
    return nlp


def main(model=None, output_dir=None, n_iter=100):
    # """Load the model, set up the pipeline and train the entity recognizer."""
    # if model is not None:
    #     nlp = spacy.load(model)  # load existing spaCy model
    #     print("Loaded model '%s'" % model)
    # else:
    #     nlp = spacy.blank("en")  # create blank Language class
    #     # nlp = spacy.load("en")  # create blank Language class
    #     print("Created blank 'en' model")
    #
    # # create the built-in pipeline components and add them to the pipeline
    # # nlp.create_pipe works for built-ins that are registered with spaCy
    # if "ner" not in nlp.pipe_names:
    #     ner = nlp.create_pipe("ner")
    #     nlp.add_pipe(ner, last=True)
    # # otherwise, get it so we can add labels
    # else:
    #     ner = nlp.get_pipe("ner")
    #
    # # add labels
    # for _, annotations in TRAIN_DATA:
    #     for ent in annotations.get("entities"):
    #         ner.add_label(ent[2])
    #
    # # get names of other pipes to disable them during training
    # pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
    # other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
    # # only train NER
    # with nlp.disable_pipes(*other_pipes), warnings.catch_warnings():
    #     # show warnings for misaligned entity spans once
    #     warnings.filterwarnings("once", category=UserWarning, module='spacy')
    #
    #     # reset and initialize the weights randomly – but only if we're
    #     # training a new model
    #     if model is None:
    #         nlp.begin_training()
    #     for itn in range(n_iter):
    #         # random.shuffle(TRAIN_DATA)
    #         losses = {}
    #         # batch up the examples using spaCy's minibatch
    #         batches = minibatch(TRAIN_DATA, size=compounding(4.0, 32.0, 1.001))
    #         for batch in batches:
    #             texts, annotations = zip(*batch)
    #             nlp.update(
    #                 texts,  # batch of texts
    #                 annotations,  # batch of annotations
    #                 drop=0.5,  # dropout - make it harder to memorise data
    #                 losses=losses,
    #             )
    #         # print("Losses", losses)
    #
    nlp = train_spacy()

    # test the trained model
    for text, _ in TRAIN_DATA:
        print("Text:", text)
        doc = nlp(text)
        print("Entities", [(ent.text, ent.label_) for ent in doc.ents])
        print("Tokens", [(t.text, t.ent_type_, t.ent_iob) for t in doc])

    # save model to output directory
    if output_dir is not None:
        output_dir = Path(output_dir)
        if not output_dir.exists():
            output_dir.mkdir()
        nlp.to_disk(output_dir)
        print("Saved model to", output_dir)

        # test the saved model
        print("Loading from", output_dir)
        nlp2 = spacy.load(output_dir)
        for text, _ in TRAIN_DATA:
            doc = nlp2(text)
            print("doc.ents:", doc.ents)
            print("Entities", [(ent.text, ent.label_) for ent in doc.ents])
            print("Tokens", [(t.text, t.ent_type_, t.ent_iob) for t in doc])

    for i in range(0, 100):
        test_text = input("Enter your testing text: ")
        doc = nlp(test_text)
        for ent in doc.ents:
            print(ent.text, ent.start_char, ent.end_char, ent.label_)


def test():
    sentence = 'Python programming is fun.'
    s = list(sentence)
    sub = 'Python'
    # result = sentence.index('is fun',)
    i = sentence.index(sub)
    k = i
    for j in range(i, len(sentence)):
        # print(k, sentence[j])
        if re.search('[ \n]', sentence[j]):
            break
        else:
            s[k] = '*'
        k += 1
    sentence = "".join(s)
    # j = i + len(sub)-1

    print(i, k, sentence)

    # result = sentence.index('Java')
    # print("Substring 'Java':", result)


if __name__ == "__main__":
    # plac.call(main)
    test()

