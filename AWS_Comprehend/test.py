#!/usr/bin/env python
import json
import plac
file1 = "./Classification/spam_keyphrases_output.json"


def main():
    with open(file1, "r") as f:
        filecontent = f.readlines()
    lgt = len(filecontent)
    print(lgt)
    for line in filecontent:
        d = json.loads(line)
        a = d['KeyPhrases']
        for b in a:
            print(b['Text'])
        break

if __name__ == "__main__":
    plac.call(main)

