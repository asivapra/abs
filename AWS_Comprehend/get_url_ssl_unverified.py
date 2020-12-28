#!/usr/bin/env python

from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import ssl

def get_soup_bs4(url):
    """
    Get the page content displayed as plain HTML.
    :param url: The page URL
    :return: soup = The page content
    """
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url, headers=hdr)
    context = ssl._create_unverified_context()
    page = urlopen(req, context=context)
    soup = BeautifulSoup(page, "html.parser")
    return soup


def user_comments_1():
    url = "http://eagle1023.webgenie.com/AWS_Comprehend/HTML/user_comments_1.html"
    soup = get_soup_bs4(url)
    # print(soup)
    review_list = soup.find_all("li", {"class": "review-list__item"})
    print(len(review_list))
    for i in review_list:
        comments = i.find("div", {"class": "review__content"})
        print(comments.text)


def user_comments_2():
    url = "http://eagle1023.webgenie.com/AWS_Comprehend/HTML/user_comments_2.html"
    soup = get_soup_bs4(url)
    tbody = soup.find("tbody", {})
    # print(len(tbody))
    k = 0
    for i in tbody:
        k += 1
        cid = "fdbk-comment-" + str(k)
        # print(cid)
        try:
            comment = i.find_all("span", {"data-test-id": cid})
            line = comment[0].text.strip().replace("A+", "").replace("+", "")
            print(line)
        except AttributeError:
            pass
        # break
        # comments = i.find("div", {"class": "card__feedback"})
        # print(comments.text)


def main():
    # user_comments_1()
    user_comments_2()

main()
