# -*- coding: utf-8 -*-
import random
from imp import reload
from random import *
import pickle
import urllib
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import Join, MapCompose, TakeFirst
from scrapy.pipelines.images import ImagesPipeline
from scrapy.http import FormRequest
from scrapy.utils.response import open_in_browser
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from scrapy.http import Request
from bs4 import BeautifulSoup
import requests
from requests.exceptions import ConnectionError
import numpy
import datetime
import re
from random import uniform
from PIL import Image, ImageEnhance
from PIL import ImageFilter
from pytesseract import *
from selenium.webdriver import ActionChains
from selenium.webdriver.support.expected_conditions import staleness_of
from six.moves import input as raw_input
# from scipy.ndimage.filters import gaussian_filter
import time
import os
from ..items import *
# from goto import goto, label
# from captcha_solver import CaptchaSolver
from twocaptchaapi import TwoCaptchaApi
from tbselenium.tbdriver import TorBrowserDriver
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.keys import Keys
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class OnionSpider(scrapy.Spider):
    name = 'onion'
    allowed_domains = []
    start_urls = ['http://***.onion']
    empire_official_url = 'http://***.onion'
    # File Location
    workDir = os.getcwd()
    web_name = '****'
    start_page_file = workDir
    log_file = workDir + '/log.txt'
    start_title = workDir + '/menu.txt'
    category_file = workDir + '/category.txt'
    # For allowing urllib & requests communicate with Tor
    proxy = {
        "http": "socks5h://127.0.0.1:9050"
    }

    # Login Info
    UN = ['*****']
    PW = ['*****']

    # Data analysis
    month_r = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May",
               "06": "Jun", "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct",
               "11": "Nov", "12": "Dec"}
    tday = datetime.date.today()
    current_year = str(tday).split("-")[0]
    current_month = str(tday).split("-")[1]
    if int(current_month) > 10:
        last_month = str(int(current_month) - 1)
    else:
        last_month = '0' + str(int(current_month) - 1)
    last_month = month_r[last_month]
    need_to_re_login = workDir + '/need_to_re_login.txt'

    def parse(self, response):
        try:
            empire = response.xpath('.//h4/a[text()[contains(., "****")]]/@href').extract_first()
            if '../' in empire:
                empire = empire.replace('..', '')
            empire_res = response.urljoin(empire)
            print("[Directing to page contains ****'s Url!]")
            yield scrapy.Request(empire_res, callback=self.get_url, dont_filter=True)
        except ConnectionError as ce:
            print(ce)
            print("[ConnectionError when directing to page contains ****'s Url!]")
        except Exception as e:
            print(e)
            print("[Error when directing to page contains ****'s Url!]")

    def get_url(self, response):
        try:
            empire_url = response.xpath('.//td[@class="url status1"]/code/text()').extract_first()
            print("[Got **** Url: {}]".format(empire_url))
            with open(self.need_to_re_login, 'w') as file:
                file.write('False')
            yield scrapy.Request(self.empire_official_url, headers={'Referer': None}, callback=self.login_page,
                                 dont_filter=True)
        except ConnectionError as ce:
            print(ce)
            print("[ConnectionError when getting ****'s Url!]")
        except Exception as e:
            print(e)
            print("[Error when getting ****'s Url!]")

    def login_page(self, response):
        try:
            current_url = response.request.url
            print("[Got the Login Page!: {}]".format(current_url))
            captcha_url = response.xpath('.//div[@class="image"]/img/@src').extract_first()
            print("[Got the CAPTCHA!: {}]".format(captcha_url))
            login_cap = requests.get(captcha_url, proxies=self.proxy)
            captcha_data = login_cap.content
            with open(self.workDir + '/captcha.jpg', 'wb') as output:
                output.write(captcha_data)
            api = TwoCaptchaApi('cb39bb1f31e79013429c8ef8676b3eb0')
            with open(self.workDir + '/captcha.jpg', 'rb') as captcha_file:
                captcha = api.solve(captcha_file)
                result = captcha.await_result()
            print(result)
            username = response.xpath('.//input[@placeholder="Username"]/@name').extract_first()
            password = response.xpath('.//input[@placeholder="Password"]/@name').extract_first()
            captcha_f = response.xpath('.//input[@placeholder="What\'s the captcha?"]/@name').extract_first()
            return FormRequest.from_response(response, formdata={
                username: '****',
                password: '****',
                captcha_f: str(result)
            }, clickdata={
                'type': 'submit',
                'class': 'primary_button'
            }, callback=self.home_page)
        except Exception as e:
            print(e)
            print("[Error when handling login form!]")

    def home_page(self, response):
        current_url = response.request.url
        print("[Got the Home Page!: {}]".format(current_url))
        Category = CategoryItem()
        main_menu = response.xpath('.//ul[@class="mainmenu"]/li/a/@href').extract()
        main_menu = [(x.strip()).encode('ascii', 'ignore') for x in main_menu]
        print(main_menu)
        Category['main_menu_url'] = main_menu

        title_menu = response.xpath('.//ul[@class="mainmenu"]/li/a/text()').extract()
        title_menu = [(x.strip()).encode('ascii', 'ignore') for x in title_menu]
        print(title_menu)
        Category['main_menu_title'] = title_menu
        number_of_title = len(title_menu)

        main_product_quantity = response.xpath('.//ul[@class="mainmenu"]/li/a/span/text()').extract()
        main_product_quantity = [(x.strip()).encode('ascii', 'ignore') for x in main_product_quantity]
        print(main_product_quantity)
        Category['main_menu_quantity'] = main_product_quantity
        time.sleep(5)

        current_time = time.time()
        date = str(datetime.datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S'))
        with open(self.category_file, 'a') as file:
            file.write(date + '\n')
            for inde, tt in enumerate(title_menu):
                file.write("Category: " + tt + " Quantity: " +
                           main_product_quantity[inde] + " Url: " + main_menu[inde] + '\n')

        for index, t in enumerate(title_menu):
            if not os.path.exists(self.start_page_file + '/' + str(index) + '.txt'):
                with open(self.start_page_file + '/' + str(index) + '.txt', 'w') as file:
                    file.write('1')

        for ind, title_url in enumerate(main_menu):
            time.sleep(1)
            count_1 = 1
            yield scrapy.Request(title_url, callback=self.crawl_each_category,
                                 meta={'ind': ind, 'url': title_url, 'count': count_1},
                                 dont_filter=True)

    def crawl_each_category(self, response):
        current_url = response.request.url
        print("[Currently crawling category's url is: {}]".format(current_url))
        if current_url != response.meta['url']:
            count_1 = response.meta['count']
            if count_1 < 9:
                count_1 += 1
                yield scrapy.Request(response.meta['url'], callback=self.crawl_each_category,
                                     meta={'ind': response.meta['ind'], 'url': response.meta['url'], 'count': count_1},
                                     dont_filter=True)
                return
            else:
                print("[Having tried nine times for getting {}]".format(response.meta['url']))
        ind = str(response.meta['ind'])
        with open(self.start_page_file + '/' + ind + '.txt', 'r') as start_p:
            for line in start_p:
                last_asc_page = int(line)
        start_time = time.time()
        date = str(datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))
        with open(self.log_file, 'a') as file:
            file.write(date + "Current menu: " + ind + ", Start page: " + str(last_asc_page) + '\n')

        asc_page = response.xpath('.//div/a[text()[contains(., "Price Low to High")]]/@href').extract_first()
        print("[Got category {}'s asc page: {}]".format(ind, asc_page))
        if last_asc_page > 1:
            asc_page = asc_page + '/' + str(15 * (last_asc_page - 1))
            asc_page2 = current_url.split('/')[:-1] + '/price_asc/' + str(15 * (last_asc_page - 1))
            if asc_page != asc_page2:
                print("[Should get asc url: {} but get: {}]".format(asc_page2, asc_page))
        count_1 = 1
        yield scrapy.Request(asc_page, callback=self.crawl_page_asc,
                             meta={'last_asc_page': last_asc_page,
                                   'ind': ind, 'url': asc_page, 'count': count_1}, dont_filter=True)

    def crawl_page_asc(self, response):
        last_asc_page = response.meta.get('last_asc_page')
        ind = response.meta.get('ind')
        current_url = response.request.url
        print("[Currently crawling category's url is: {}]".format(current_url))
        if current_url != response.meta['url']:
            count_2 = response.meta['count']
            if count_2 < 9:
                count_2 += 1
                yield scrapy.Request(response.meta['url'], callback=self.crawl_page_asc,
                                     meta={'last_asc_page': last_asc_page,
                                           'ind': ind, 'url': response.meta['url'], 'count': count_2}, dont_filter=True)
                return
            else:
                print("[Having tried nine times for getting {}]".format(response.meta['url']))
        end_page = response.xpath('.//ul[@class="pagination"]/li/a[text()[contains(., "Last")]]').xpath(
            '@data-ci-pagination-page').extract_first()
        end_page = int(end_page)
        with open(self.workDir + '/endpage.txt', 'a') as end_file:
            end_file.write("[Category " + str(ind) + " End page: " + str(end_page) + "]\n")
        # For the test
        # --------------- #
        # end_page = 1
        # --------------- #

        print("[Category {} End page: {}]".format(ind, end_page))
        count = last_asc_page
        if count <= end_page:
            print("[Start crawling category: {} ASC PAGE: {}]".format(ind, count))
            start_time = time.time()
            date = str(datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))
            with open(self.log_file, 'a') as file:
                file.write(date + "Current menu: " + ind + ", Now page: " + str(count) + '\n')
            sales = []
            left_quantity = []
            drug_url = response.xpath('.//div[@class="col-1search"]/a/@href').extract()
            drug_url = [x.encode('ascii', 'ignore') for x in drug_url]
            is_fifteen = len(drug_url)
            print(drug_url)
            drug_name = response.xpath('.//div[@class="col-1centre"]/div/a/text()').extract()
            drug_name = [x.encode('ascii', 'ignore') for x in drug_name]
            print(drug_name)

            drug_cat = response.xpath('.//div[@class="col-1centre"]/div/p/text()').extract()
            drug_category = self.format_list(drug_cat)

            drug_class = []
            drug_id = []
            vendor_product_quantity = []
            for i, content in enumerate(drug_category):
                if i % 2 == 0:
                    drug_class.append(content.split('-')[1])
                    drug_id.append(content.split('-')[0])
                else:
                    vendor_product_quantity.append(content)

            vendor_url = response.xpath('.//div[@class="col-1centre"]/div/p/a/@href').extract()
            vendor_url = [x.encode('ascii', 'ignore') for x in vendor_url]

            vendor_name = response.xpath('.//div[@class="col-1centre"]/div/p/a/text()').extract()
            vendor_name = [x.encode('ascii', 'ignore') for x in vendor_name]

            product_quantity = response.xpath('.//div[@class="head"]/text()').extract()
            for i, content in enumerate(product_quantity):
                product_quantity[i] = content.strip()
            product_quantity = filter(None, product_quantity)
            for i, content in enumerate(product_quantity):
                if i % 3 == 1:
                    sales.append(str(content.encode('ascii', 'ignore')))
                if i % 3 == 2:
                    left_quantity.append(str(content.encode('ascii', 'ignore')))

            product_price_usd = response.xpath('.//div[@class="col-1right"]/p/a/text()').extract()
            product_price_usd = [x.encode('ascii', 'ignore') for x in product_price_usd]

            next_url = response.xpath('.//ul[@class="pagination"]/li/a[@rel="next"]/@href').extract_first()

            # Get all items on the age
            for index, item_url in enumerate(drug_url):
                time.sleep(1)
                print("[Current crawling category: {}, ASC PAGE: {}, item: {}]".format(
                    ind, count, index + 1))
                count_3 = 1
                metadata = {'drug_name': drug_name[index],
                            'drug_class': drug_class[index],
                            'drug_id': drug_id[index],
                            'vendor_product_quantity': vendor_product_quantity[index],
                            'vendor_name': vendor_name[index],
                            'vendor_url': vendor_url[index],
                            'sales': sales[index],
                            'left_quantity': left_quantity[index],
                            'product_price_usd': product_price_usd[index],
                            'url': item_url,
                            'count': count_3
                            }

                yield scrapy.Request(item_url, callback=self.crawl_item, meta=metadata, dont_filter=True)

            count += 1
            with open(self.start_page_file + '/' + str(ind) + '.txt', 'w') as file:
                file.write(str(count))

            yield scrapy.Request(next_url, callback=self.crawl_page_asc,
                                 meta={'last_asc_page': count,
                                       'ind': ind, 'url': next_url, 'count': 1}, dont_filter=True)

    def crawl_item(self, response):
        Product = ProductItem()
        current_url = response.request.url
        print("[Currently crawling item's url is: {}]".format(current_url))
        print("[Response url is: {}]".format(response.meta['url']))
        if current_url != response.meta['url']:
            if response.meta['count'] < 9:
                response.meta['count'] += 1
                yield scrapy.Request(response.meta['url'], callback=self.crawl_page_asc,
                                     meta=response.meta, dont_filter=True)
                return
            else:
                print("[Having tried nine times for getting {}]".format(response.meta['url']))

        sold_since = response.xpath(
            './/div[@class="listDes"]/p/span[1]/text()').extract_first()
        sold_since_1 = sold_since.split()[-3:]
        sold_since_1[0] = list(self.month_r.keys())[list(self.month_r.values()).index(sold_since_1[0][0:3])]
        sold_since_1[1] = sold_since_1[1][0:2]
        sold_since = [sold_since_1[2], sold_since_1[0], sold_since_1[1]]
        sold_since = "".join(sold_since)
        sold_since = sold_since.encode('ascii', 'ignore')
        print(sold_since)
        vendor_sales_rating = response.xpath(
            './/div[@class="listDes"]/p/span[4]/text()').extract()
        if not vendor_sales_rating:
            vendor_sales_rating = "No Record"
        else:
            for x in vendor_sales_rating:
                vendor_sales_rating = x.encode('ascii', 'ignore')
        print(vendor_sales_rating)
        product_package = response.xpath('.//table/tbody/tr[1]/td[2]/text()').extract_first()
        product_origin = response.xpath('.//table/tbody/tr[1]/td[4]/text()').extract_first()
        product_destination = response.xpath('.//table/tbody/tr[2]/td[4]/text()').extract_first()
        product_payment = response.xpath('.//table/tbody/tr[3]/td[4]/text()').extract_first()
        print(product_origin)
        print(product_destination)

        if (product_origin is None) or (not product_origin):
            product_origin = 'Not Found'
        if (product_destination is None) or (not product_destination):
            product_destination = 'Not Found'
        if (product_package is None) or (not product_package):
            product_package = 'Not Found'
        if (product_payment is None) or (not product_payment):
            product_payment = 'Not Found'

        Product['product_name'] = response.meta['drug_name']
        Product['product_category'] = response.meta['drug_class']
        Product['vendor_business_scope'] = response.meta['vendor_product_quantity']
        Product['product_vendor_url'] = response.meta['vendor_url']
        Product['product_vendor'] = response.meta['vendor_name']
        Product['product_left_quantity'] = response.meta['left_quantity']
        Product['product_price_usd'] = response.meta['product_price_usd']
        Product['product_sales'] = response.meta['sales']
        Product['product_id'] = response.meta['drug_id']
        Product['sold_since'] = sold_since
        Product['vendor_rating'] = vendor_sales_rating
        Product['product_origin'] = product_origin
        Product['product_destination'] = product_destination
        Product['product_transmit'] = product_package
        Product['product_payment_method'] = product_payment

        # Get feedback
        feedback_url = response.xpath('.//div[@class="tab"]/a[text()[contains(., "Feedback")]]/@href').extract_first()

        yield scrapy.Request(feedback_url, callback=self.handle_feedback, meta={'Product': Product},
                             dont_filter=True)

    def handle_feedback(self, response):
        current_url = response.request.url
        if current_url.split('/')[-1] == 'login':
            with open(self.need_to_re_login, 'w') as file:
                file.write('True')
                time.sleep(60)
            yield scrapy.Request(self.empire_official_url, callback=self.login_page, dont_filter=True)
            return
        revenue_total = 0.0
        revenue_last_month = 0.0
        Product = response.meta['Product']
        feedback_total = []

        total = response.xpath(
            './/div[@class="right-content"]/div[@class="tabcontent"]/'
            'p[@class="boldstats"]/text()').extract_first()
        positive = response.xpath(
            './/div[@class="right-content"]/div[@class="tabcontent"]/'
            'p[@class="boldstats"]/font[1]/text()').extract_first()
        negative = response.xpath(
            './/div[@class="right-content"]/div[@class="tabcontent"]/'
            'p[@class="boldstats"]/font[2]/text()').extract_first()
        neutral = response.xpath(
            './/div[@class="right-content"]/div[@class="tabcontent"]/'
            'p[@class="boldstats"]/font[3]/text()').extract_first()

        if total is None:
            print("[No feedback yet]")
            total = 0
            positive = 0
            negative = 0
            neutral = 0
            Product['total_feedback'] = total
            Product['positive_fb'] = positive
            Product['negative_fb'] = negative
            Product['neutral_fb'] = neutral
            Product['product_total_revenue'] = revenue_total
            Product['product_last_month_revenue'] = revenue_last_month

            yield Product

        else:
            print("[Find Feedback]")
            positive = int(positive.split()[-1])
            negative = int(negative.split()[-1])
            neutral = int(neutral.split()[-1])
            total = positive + negative + neutral
            print(total)
            product_feedback = response.xpath(
                './/table[contains(@class, "user_feedbackTbl")]/tr')
            print("[number of feedback per page: {}]".format(len(product_feedback)))
            for i, tr in enumerate(product_feedback):
                if i == 0:
                    continue
                sold_price = tr.xpath('td[3]/p/text()').extract_first()
                sold_price = sold_price.split()[-1]
                if ',' in sold_price:
                    sold_price = sold_price.replace(',', '')
                sold_price = float(sold_price.encode('ascii', 'ignore'))
                date_month = tr.xpath('td[4]/p/text()').extract_first()
                month = date_month.split()[0]
                date_year = date_month.split()[-1]
                revenue_total += sold_price
                if month == self.last_month and date_year == self.current_year:
                    revenue_last_month += sold_price

            print("[revenue total: {} revenue last: {}]".format(revenue_total, revenue_last_month))

            Product['total_feedback'] = total
            Product['positive_fb'] = positive
            Product['negative_fb'] = negative
            Product['neutral_fb'] = neutral

            meta_feedback = {'rev_last': revenue_last_month,
                             'rev_total': revenue_total,
                             'Product': Product}

            get_next_feedback = response.xpath(
                './/ul[@class="pagination"]/li/a[@rel="next"]').xpath(
                '@href').extract_first()
            if get_next_feedback is not None:
                print("[Got Next Feedback Page!]")
                with open(self.need_to_re_login, 'r') as file:
                    for line in file:
                        if 'True' in line:
                            time.sleep(60)
                            yield scrapy.Request(self.empire_official_url, callback=self.login_page, dont_filter=True)
                            return
                        else:
                            yield scrapy.Request(get_next_feedback, callback=self.get_next_feedback, meta=meta_feedback,
                                                 dont_filter=True)

            else:
                print("[Last Page! No Next Feedback Page Left!]")
                Product['product_total_revenue'] = revenue_total
                Product['product_last_month_revenue'] = revenue_last_month

                yield Product

    def get_next_feedback(self, response):
        current_url = response.request.url
        if current_url.split('/')[-1] == 'login':
            with open(self.need_to_re_login, 'w') as file:
                file.write('True')
                time.sleep(60)
            yield scrapy.Request(self.empire_official_url, callback=self.login_page, dont_filter=True)
            return
        Product = response.meta['Product']
        revenue_last_month = float(response.meta['rev_last'])
        revenue_total = float(response.meta['rev_total'])
        print("[revenue total: {} revenue last: {}]".format(revenue_total, revenue_last_month))
        product_feedback = response.xpath(
            './/table[contains(@class, "user_feedbackTbl")]/tr')

        for i, tr in enumerate(product_feedback):
            if i == 0:
                continue
            sold_price = tr.xpath('td[3]/p/text()').extract_first()
            sold_price = sold_price.split()[-1]
            if ',' in sold_price:
                sold_price = sold_price.replace(',', '')
            sold_price = float(sold_price.encode('ascii', 'ignore'))
            date_month = tr.xpath('td[4]/p/text()').extract_first()
            month = date_month.split()[0]
            date_year = date_month.split()[-1]
            revenue_total += sold_price
            if month == self.last_month and date_year == self.current_year:
                revenue_last_month += sold_price

        meta_feedback = {'rev_last': str(revenue_last_month),
                         'rev_total': str(revenue_total),
                         'Product': Product}

        get_next_feedback = response.xpath(
            './/ul[@class="pagination"]/li/a[@rel="next"]').xpath(
            '@href').extract_first()
        if get_next_feedback is not None:
            print("[Got Next Feedback Page!]")
            with open(self.need_to_re_login, 'r') as file:
                for line in file:
                    if 'True' in line:
                        time.sleep(60)
                        yield scrapy.Request(self.empire_official_url, callback=self.login_page, dont_filter=True)
                        return
                    else:
                        yield scrapy.Request(get_next_feedback, callback=self.get_next_feedback, meta=meta_feedback)

        else:
            print("[Last Page! No Next Feedback Page Left!]")
            Product['product_total_revenue'] = revenue_total
            Product['product_last_month_revenue'] = revenue_last_month

            yield Product

    def format_list(self, unformatted_list):
        formatted_list = []
        for i in unformatted_list:
            i = ''.join(i.split())
            formatted_list.append(str(i.encode('ascii', 'ignore')))
        return formatted_list
