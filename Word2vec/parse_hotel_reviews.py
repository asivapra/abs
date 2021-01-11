#!/usr/bin/env python
"""
Project: ABS
Git: https://github.com/asivapra/abs
Author: Arapaut V. Sivaprasad
Created: 10/01/2021
Last Modified: 10/01/2021

Description:
    - To parse a HTMl for hotel reviews from Trevago

"""
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import time

retries = 0  # Number of times to try and retrieve a page.
url = "https://eagle1023.webgenie.com/ABS/Word2Vec/hotel_reviews.html"


def get_soup_bs4(url):
    """
    Get the page content displayed as plain HTML.
    :param url: The page URL
    :return: soup = The page content
    """
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url, headers=hdr)
    page = urlopen(req)
    soup = BeautifulSoup(page, "html.parser")
    return soup


def get_reviews(url):
    soup = get_soup_bs4(url)
    # print(soup)
    review_pees = soup.find_all("p", {"class": "sl-review__summary"})
    print("Number of Reviews:", len(review_pees))
    n = 0
    for i in review_pees:
        n += 1
        review = i.text
        print(review)


def main():
    ct0 = time.perf_counter()  # Track the elapsed time
    get_reviews(url)
    et = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et))


if __name__ == '__main__':
    # create_urls()  # This is required only once to get a list of URLs for working with the 'main()'
    main()
