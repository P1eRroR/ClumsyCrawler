# -*- coding: utf-8 -*-
# -------------------------------------------#
# http://zw3crggtadila2sg.onion/imageboard/  #
# need username and password                 #
import requests
from requests.exceptions import ConnectionError
import scrapy
# import socks
import os
import time
from bs4 import BeautifulSoup
from ..items import OnionwebcheckerItem
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def create_file(workDir):
    valid = workDir + "valid.txt"
    invalid = workDir + "invalid.txt"
    if not os.path.isfile(valid):
        write_file(valid, '')
    if not os.path.isfile(invalid):
        write_file(invalid, '')


def write_file(path, data):
    f = open(path, 'w')
    f.write(data)
    f.close()


class OnionwebcheckerSpider(scrapy.Spider):
    name = 'Onionwebchecker'
    allowed_domains = ['onion']
    start_urls = ['http://hiddenwikitor.com//']
    # http://zqktlwi4fecvo6ri.onion/wiki/index.php/Main_Page
    workDir = os.getcwd() + '/'
    proxy = {
        "http": "socks5h://127.0.0.1:9050"
    }
    valid = workDir + "valid.txt"
    invalid = workDir + "invalid.txt"
    valid_url = []
    invalid_url = []
    create_file(workDir)

    def parse(self, response):
        onion_website = OnionwebcheckerItem()
        onion = response.xpath('.//div[@id="post"]')
        url_selector = onion.xpath(".//a[@rel='no-follow']")
        count = 1
        start_time = time.time()
        for selector in url_selector:
            link = selector.xpath("@href").extract_first()
            print link
            print ("[Processing No.%d onion link]" % count)
            count += 1
            if link == 'http://zw3crggtadila2sg.onion/imageboard/':
                continue
            try:
                r = requests.get(link, proxies=self.proxy)
                print "This is a valid link!"
                self.valid_url.append(link)
            except ConnectionError as e:
                print "[ERROR]:"
                print e
                self.invalid_url.append("{'url':" + link + ", 'error':" + e})
        
        print("--- %s hours ---" % ((time.time() - start_time)/3600))

        print "=============valid=============="
        print self.valid_url
        print "=============invalid=============="
        print self.invalid_url

        with open(self.valid, "a") as txt_file:
            for link in self.valid_url:
                txt_file.write(link + "\n")

        with open(self.invalid, "a") as txt_file:
            for link, error in self.invalid_url:
                txt_file.write(link + "\n")
                txt_file.write(error + "\n")

