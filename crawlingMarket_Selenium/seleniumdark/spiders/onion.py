# -*- coding: utf-8 -*-
import random
import sqlite3
import urllib
import scrapy
import numpy
import datetime
import re
import requests
import time
import os
from imp import reload
from random import *
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
from requests.exceptions import ConnectionError
from random import uniform
from PIL import Image, ImageEnhance
from PIL import ImageFilter
from pytesseract import *
from selenium.webdriver import ActionChains
from selenium.webdriver.support.expected_conditions import staleness_of
from six.moves import input as raw_input
from ..items import *
from twocaptchaapi import TwoCaptchaApi
from tbselenium.tbdriver import TorBrowserDriver
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
    start_urls = ['http://darkfailllnkf4vf.onion']
    empire_url = "http://****.onion"
    # File Location
    workDir = os.getcwd()
    web_name = '*******'
    start_page_file = workDir + '/page.txt'
    log_file = workDir + '/log.txt'
    start_title = workDir + '/menu.txt'
    report = workDir + '/report.txt'
    # For allowing urllib & requests communicate with Tor
    proxy = {
        "http": "socks5h://127.0.0.1:9050"
    }

    # Login Info
    UN = ['******']
    PW = ['******']

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
    loop_main_menu = []

    def start_requests(self):
        yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)

    def start_again(self, response):
        with TorBrowserDriver('/PATH_TO->tor-browser_en-US') as driver:
            # disable javascript
            driver.get("about:config")
            actions = ActionChains(driver)
            actions.send_keys(Keys.RETURN)
            actions.send_keys("javascript.enabled")
            actions.perform()
            actions.send_keys(Keys.TAB)
            actions.send_keys(Keys.RETURN)
            actions.send_keys(Keys.F5)
            actions.perform()
            try:
                current_url = response.request.url
                driver.get(current_url)
                empire_market = driver.find_element_by_link_text('*******').click()
                wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH,
                                                                                       '//div[@class="urls"]')))
                time.sleep(2)
                start_url_selector = Selector(text=driver.page_source.encode('utf-8'))
                empire_market_url = start_url_selector.xpath('.//td[@class="url status1"]/code/text()').extract_first()
                print(empire_market_url)
                if empire_market_url is None:
                    print("[Updating valid url, try 60 seconds later!]")
                    driver.quit()
                    time.sleep(60)
                    yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                else:
                    try:
                        try:
                            res = requests.get(empire_market_url, proxies=self.proxy)
                            yield scrapy.Request(empire_market_url, headers={'Referer': None}, callback=self.parse,
                                                 dont_filter=True)
                        except ConnectionError as e:
                            print(e)
                            yield scrapy.Request(self.empire_url, headers={'Referer': None}, callback=self.parse,
                                                 dont_filter=True)
                    except TimeoutException as e:
                        print(e)
                        print("[Fail to communicate with ****!]")
                        driver.quit()
                        time.sleep(60)
                        yield scrapy.Request(self.empire_url, self.start_again, dont_filter=True)
                        return
                    except Exception as e:
                        print(e)

                        time.sleep(60)
                        yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                        return
            except Exception as e:
                print(e)
                print("[Get start url error!]")

    def parse(self, response):
        # Get page from last crawl
        if not os.path.exists(self.start_page_file):
            with open(self.start_page_file, 'w') as file:
                file.write('1')  # default 0
                last_asc_page = 1  # default 0

        if not os.path.exists(self.start_title):
            with open(self.start_title, 'w') as file:
                file.write('0')  # default 0
                last_loop_menu = 0  # default 0
        else:
            with open(self.start_title, 'r') as start_tt:
                for line in start_tt:
                    last_loop_menu = int(line)

        Product = ProductItem()
        Category = CategoryItem()
        work_url = response.request.url
        print(work_url)
        # Initialize web driver for Tor
        with TorBrowserDriver('PATH_TO_tor-browser_en-US') as driver:
            # disable javascript
            driver.get("about:config")
            actions = ActionChains(driver)
            actions.send_keys(Keys.RETURN)
            actions.send_keys("javascript.enabled")
            actions.perform()
            actions.send_keys(Keys.TAB)
            actions.send_keys(Keys.RETURN)
            actions.send_keys(Keys.F5)
            actions.perform()
            driver.set_page_load_timeout(30)
            try:
                count_login = 0
                while count_login < 10:
                    driver.get(work_url)
                    wait_login_page = WebDriverWait(driver, 30).until(ec.presence_of_element_located((
                        By.XPATH, './/input[@placeholder="Username"]')))
                    # Fill the login form
                    time.sleep(1.7)
                    pName = randint(0, len(self.UN) - 1)
                    username = driver.find_element_by_xpath('.//input[@placeholder="Username"]')
                    username.send_keys(self.UN[pName])
                    time.sleep(2)
                    password = driver.find_element_by_xpath('.//input[@placeholder="Password"]')
                    password.send_keys(self.PW[pName])
                    try:
                        login_url_selector = Selector(text=driver.page_source.encode('utf-8'))
                        captcha_url = login_url_selector.xpath('.//div[@class="image"]/img/@src').extract_first()
                        login_cap = requests.get(captcha_url, proxies=self.proxy)
                        captcha_data = login_cap.content
                        with open(self.workDir + '/captcha.jpg', 'wb') as output:
                            output.write(captcha_data)
                        api = TwoCaptchaApi('GET_YOUR_OWN_API_FROM_2CAPTCHA')
                        with open(self.workDir + '/captcha.jpg', 'rb') as captcha_file:
                            captcha = api.solve(captcha_file)
                            result = captcha.await_result()
                        print(result)
                        captcha = driver.find_element_by_xpath('.//input[contains(@placeholder, "captcha?")]')
                        captcha.send_keys(result)
                        time.sleep(1.5)
                        sub = driver.find_element_by_xpath('.//input[@type="submit"]').click()
                        break
                    except Exception as e:
                        print(e)
                        print("[CAPTCHA error]")
                    if count_login == 9:
                        driver.quit()
                        time.sleep(30)
                        yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                        return
                    count_login += 1
            except TimeoutException as te:
                print(te)
                print("[login page timeout]")
                driver.quit()
                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                return
            except WebDriverException as we:
                print(we)
                driver.quit()
                time.sleep(10)
                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                return
            except Exception as e:
                print(e)
                print("[Login Error!]")
                driver.quit()
                time.sleep(10)
                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                return

            # Home page
            try:
                wait_main = WebDriverWait(driver, 30).until(ec.presence_of_element_located((By.XPATH, '//ul['
                                                                                                      '@class'
                                                                                                      '="mainmenu"]')))
            except TimeoutException as te:
                print(te)
                yield scrapy.Request(work_url, self.parse, dont_filter=True)
                return
            time.sleep(3)
            main_selector = Selector(text=driver.page_source.encode('utf-8'))
            main_menu = main_selector.xpath('.//ul[@class="mainmenu"]/li/a/@href').extract()
            Category['main_menu_url'] = [x.encode('ascii', 'ignore') for x in main_menu]

            title_menu = main_selector.xpath('.//ul[@class="mainmenu"]/li/a/text()').extract()
            title_menu = [(x.strip()).encode('ascii', 'ignore') for x in title_menu]
            print(title_menu)
            Category['main_menu_title'] = title_menu
            number_of_title = len(title_menu)

            main_product_quantity = main_selector.xpath('.//ul[@class="mainmenu"]/li/a/span/text()').extract()
            Category['main_menu_quantity'] = [x.encode('ascii', 'ignore') for x in main_product_quantity]

            # Loop through all titles in main menu
            # for title in title_menu:

            loop_menu = last_loop_menu
            while loop_menu < number_of_title:
                with open(self.start_page_file, 'r') as start_page:
                    for line in start_page:
                        last_asc_page = int(line)
                with open(self.log_file, 'a') as file:
                    file.write("Current menu: " + str(last_loop_menu) + "Start page: " + str(last_asc_page) + '\n')
                # Find drugs page
                count_load_cate = 0
                while count_load_cate < 10:
                    try:
                        cate = driver.find_element_by_xpath(
                            './/a[text()[contains(.,' + '"' + title_menu[loop_menu] + '"' + ')]]/input').click()
                        time.sleep(2)
                        wait_right_contents = WebDriverWait(driver, 30).until(ec.presence_of_element_located(
                            (By.LINK_TEXT, 'Price Low to High')))
                        print("[Got the {} page!]".format(title_menu[loop_menu]))
                        time.sleep(2)
                        break
                    except TimeoutException as te:
                        print(te)
                        actions.send_keys(Keys.F5)
                        time.sleep(10)
                    except WebDriverException as we:
                        print(we)
                        actions.send_keys(Keys.F5)
                        time.sleep(10)
                    except Exception as e:
                        print(e)
                        print("[Loading {} page error]".format(title_menu[loop_menu]))
                        actions.send_keys(Keys.F5)
                    if count_load_cate == 9:
                        driver.quit()
                        time.sleep(120)
                        yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                        return
                    count_load_cate += 1

                # Directing to 'price low to high' page
                count_load_asc = 0
                while count_load_asc < 10:
                    try:
                        old_page = driver.find_element_by_tag_name('html')
                        price_asc = driver.find_element_by_link_text('Price Low to High').click()
                        wait_asc_page = WebDriverWait(driver, 30).until(staleness_of(old_page))
                        print("[Got the asc page!]")
                        current_asc_url = driver.current_url
                        category_number = current_asc_url.split('/')[-2]
                        print("[Current category number: {}]".format(category_number))
                        time.sleep(5)
                        scrapy_selector = Selector(text=driver.page_source.encode('utf-8'))
                        test_elements = scrapy_selector.xpath('.//div[@class="col-1search"]').extract()
                        if test_elements is None:
                            actions.send_keys(Keys.F5)
                        else:
                            break
                    except TimeoutException as te:
                        print(te)
                        actions.send_keys(Keys.F5)
                    except WebDriverException as we:
                        print(we)
                        actions.send_keys(Keys.F5)
                        time.sleep(10)
                    except Exception as e:
                        print(e)
                        print("[Loading asc page error]")
                        actions.send_keys(Keys.F5)
                    if count_load_asc == 9:
                        driver.quit()
                        time.sleep(120)
                        yield scrapy.Request(work_url, self.parse, dont_filter=True)
                        return
                    count_load_asc += 1

                end_page = scrapy_selector.xpath('.//ul[@class="pagination"]/li/a[text()[contains(., "Last")]]').xpath(
                    '@data-ci-pagination-page').extract_first()
                end_page = int(end_page)
                print("[End page: ]")
                print(end_page)

                if 'onion' not in work_url.split('/')[-1]:
                    work_url = '/'.join(work_url.split('/')[:-1])
                    print(work_url)

                count = 1
                if count < last_asc_page:
                    get_current_crawl_url = work_url + '/category/categories/' + str(category_number) + '' \
                                                                                                        '/price_asc/' + str(
                        15 * (last_asc_page - 1))
                    count_last_page = 0
                    while count_last_page < 10:
                        try:
                            old_page = driver.find_element_by_tag_name('html')
                            driver.get(get_current_crawl_url)
                            wait_asc_page = WebDriverWait(driver, 30).until(staleness_of(old_page))
                            print("[get current crawling {} ASC PAGE: {}]".format(title_menu[loop_menu], last_asc_page))
                            count = last_asc_page
                            time.sleep(3)
                            scrapy_selector = Selector(text=driver.page_source.encode('utf-8'))
                            test_elements = scrapy_selector.xpath('.//div[@class="col-1search"]').extract()
                            if test_elements is None:
                                actions.send_keys(Keys.F5)
                            else:
                                break
                        except TimeoutException as te:
                            print(te)
                            actions.send_keys(Keys.F5)
                        except WebDriverException as we:
                            print(we)
                            actions.send_keys(Keys.F5)
                            time.sleep(10)
                        except Exception as e:
                            print(e)
                            print("[Loading current crawling page error]")
                            actions.send_keys(Keys.F5)
                        if count_last_page == 9:
                            driver.quit()
                            time.sleep(120)
                            yield scrapy.Request(work_url, self.parse, dont_filter=True)
                            return
                        count_last_page += 1

                # Start crawling
                # end_page = 1
                while count <= end_page:
                    print("[Start crawling {} ASC PAGE: {}]".format(title_menu[loop_menu], count))
                    # Define variables--page base
                    product_trans = []
                    origin = []
                    destination = []
                    payment_method = []
                    sales = []
                    left_quantity = []
                    sold_date = []
                    total_rev = []
                    last_rev = []
                    rating = []
                    feedback_total = []
                    feedback_posi = []
                    feedback_nega = []
                    feedback_neut = []

                    # scrape 15 elements on the page
                    drug_elements = scrapy_selector.xpath('.//div[@class="col-1search"]').extract()
                    drug_elements = [x.encode('ascii', 'ignore') for x in drug_elements]
                    is_fifteen = len(drug_elements)
                    drug_url = scrapy_selector.xpath('.//div[@class="col-1search"]/a/@href').extract()
                    drug_url = [x.encode('ascii', 'ignore') for x in drug_url]
                    print(drug_url)

                    drug_name = scrapy_selector.xpath('.//div[@class="col-1centre"]/div/a/text()').extract()
                    drug_name = [x.encode('ascii', 'ignore') for x in drug_name]
                    print(drug_name)

                    drug_cat = scrapy_selector.xpath('.//div[@class="col-1centre"]/div/p/text()').extract()
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

                    vendor_url = scrapy_selector.xpath('.//div[@class="col-1centre"]/div/p/a/@href').extract()
                    vendor_url = [x.encode('ascii', 'ignore') for x in vendor_url]

                    vendor_name = scrapy_selector.xpath('.//div[@class="col-1centre"]/div/p/a/text()').extract()
                    vendor_name = [x.encode('ascii', 'ignore') for x in vendor_name]

                    product_quantity = scrapy_selector.xpath('.//div[@class="head"]/text()').extract()
                    for i, content in enumerate(product_quantity):
                        product_quantity[i] = content.strip()
                    product_quantity = filter(None, product_quantity)
                    for i, content in enumerate(product_quantity):
                        if i % 3 == 1:
                            sales.append(str(content.encode('ascii', 'ignore')))
                        if i % 3 == 2:
                            left_quantity.append(str(content.encode('ascii', 'ignore')))

                    product_price_usd = scrapy_selector.xpath('.//div[@class="col-1right"]/p/a/text()').extract()
                    product_price_usd = [x.encode('ascii', 'ignore') for x in product_price_usd]

                    # Get next page
                    next_url = scrapy_selector.xpath('.//ul[@class="pagination"]/li/a[@rel="next"]').xpath(
                        '@href').extract_first()

                    # Get item info
                    loop_item = 0
                    while loop_item < is_fifteen:
                        print("[Current crawling {} ASC PAGE: {}, Loop item: {}]".format(
                            title_menu[loop_menu], count, loop_item + 1))
                        count_every_loop_item = 0
                        while count_every_loop_item < 10:
                            try:
                                old_page = driver.find_element_by_tag_name('html')
                                driver.get(drug_url[loop_item])
                                wait_item_page = WebDriverWait(driver, 30).until(staleness_of(old_page))
                                time.sleep(2)
                                drug_page_selector = Selector(text=driver.page_source.encode('utf-8'))
                                sold_since = drug_page_selector.xpath(
                                    './/div[@class="listDes"]/p/span[1]/text()').extract_first()
                                if sold_since is None:
                                    actions.send_keys(Keys.F5)
                                else:
                                    print("[Got item {}'s page]".format(loop_item + 1))
                                    break
                            except TimeoutException as te:
                                print(te)
                                actions.send_keys(Keys.F5)
                            except WebDriverException as we:
                                print(we)
                                actions.send_keys(Keys.F5)
                                time.sleep(10)
                            except Exception as e:
                                print("[Loop item problem]")
                                print(e)
                            if count_every_loop_item == 9:
                                driver.quit()
                                time.sleep(120)
                                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                                return
                            count_every_loop_item += 1

                        # Format sold_since
                        sold_since_1 = sold_since.split()[-3:]
                        sold_since_1[0] = list(self.month_r.keys())[
                            list(self.month_r.values()).index(sold_since_1[0][0:3])]
                        sold_since_1[1] = sold_since_1[1][0:2]
                        sold_since = [sold_since_1[2], sold_since_1[0], sold_since_1[1]]
                        sold_since = "".join(sold_since)
                        sold_since = sold_since.encode('ascii', 'ignore')
                        print(sold_since)
                        vendor_sales_rating = drug_page_selector.xpath(
                            './/div[@class="listDes"]/p/span[4]/text()').extract()
                        if not vendor_sales_rating:
                            vendor_sales_rating = "No Record"
                        else:
                            for x in vendor_sales_rating:
                                vendor_sales_rating = x.encode('ascii', 'ignore')
                        print(vendor_sales_rating)
                        product_package = drug_page_selector.xpath(
                            './/table/tbody/tr[1]/td[2]/text()').extract_first()
                        product_origin = drug_page_selector.xpath(
                            './/table/tbody/tr[1]/td[4]/text()').extract_first()
                        product_destination = drug_page_selector.xpath(
                            './/table/tbody/tr[2]/td[4]/text()').extract_first()
                        product_payment = drug_page_selector.xpath(
                            './/table/tbody/tr[3]/td[4]/text()').extract_first()
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

                        sold_date.append(sold_since)
                        rating.append(vendor_sales_rating)
                        product_trans.append(product_package.encode('ascii', 'ignore'))
                        origin.append(product_origin.encode('ascii', 'ignore'))
                        destination.append(product_destination.encode('ascii', 'ignore'))
                        payment_method.append(product_payment.encode('ascii', 'ignore'))

                        # Variables for individual item
                        revenue_total = 0.0
                        revenue_last_month = 0.0

                        count_get_fb = 0
                        while count_get_fb < 10:
                            try:
                                old_page = driver.find_element_by_tag_name('html')
                                feedback_url = driver.find_element_by_link_text('Feedback').click()
                                wait_feedback_page = WebDriverWait(driver, 30).until(staleness_of(old_page))
                                break
                            except TimeoutException as te:
                                print(te)
                                actions.send_keys(Keys.F5)
                            except WebDriverException as e:
                                print(e)
                                actions.send_keys(Keys.F5)
                            if count_get_fb == 9:
                                driver.quit()
                                time.sleep(30)
                                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                                return
                            count_get_fb += 1
                        time.sleep(1)
                        feedback_selector = Selector(text=driver.page_source.encode('utf-8'))
                        if len(feedback_total) < loop_item + 1:
                            total = feedback_selector.xpath(
                                './/div[@class="right-content"]/div[@class="tabcontent"]/'
                                'p[@class="boldstats"]/text()').extract_first()
                            positive = feedback_selector.xpath(
                                './/div[@class="right-content"]/div[@class="tabcontent"]/'
                                'p[@class="boldstats"]/font[1]/text()').extract_first()
                            negative = feedback_selector.xpath(
                                './/div[@class="right-content"]/div[@class="tabcontent"]/'
                                'p[@class="boldstats"]/font[2]/text()').extract_first()
                            neutral = feedback_selector.xpath(
                                './/div[@class="right-content"]/div[@class="tabcontent"]/'
                                'p[@class="boldstats"]/font[3]/text()').extract_first()

                            if total is None:
                                print("[No feedback yet]")
                                feedback_total.append(0)
                                feedback_posi.append(0)
                                feedback_nega.append(0)
                                feedback_neut.append(0)
                            else:
                                print("[Find Feedback]")
                                feedback_posi.append(int(positive.split()[-1]))
                                feedback_nega.append(int(negative.split()[-1]))
                                feedback_neut.append(int(neutral.split()[-1]))
                                total_num = int(positive.split()[-1]) + int(negative.split()[-1]) + int(
                                    neutral.split()[-1])
                                print(total_num)
                                feedback_total.append(total_num)

                                while True:
                                    product_feedback = feedback_selector.xpath(
                                        './/table[contains(@class, "user_feedbackTbl")]/tbody/tr')

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

                                    get_next_feedback = feedback_selector.xpath(
                                        './/ul[@class="pagination"]/li/a[@rel="next"]').xpath(
                                        '@href').extract_first()
                                    if get_next_feedback is not None:
                                        print("[Got Next Feedback Page!]")
                                        count_get_next_fb = 0
                                        while count_get_next_fb < 10:
                                            try:
                                                old_page = driver.find_element_by_tag_name('html')
                                                driver.get(get_next_feedback)
                                                wait_feedback_page = WebDriverWait(driver, 30).until(staleness_of(
                                                    old_page))
                                                time.sleep(1)
                                                feedback_selector = Selector(text=driver.page_source.encode('utf-8'))
                                                test_feedback = feedback_selector.xpath(
                                                    './/table[contains(@class, "user_feedbackTbl")]/tbody/tr')
                                                if test_feedback is None:
                                                    actions.send_keys(Keys.F5)
                                                else:
                                                    print("[Show next feedback page!]")
                                                    break
                                            except TimeoutException as te:
                                                print(te)
                                                actions.send_keys(Keys.F5)
                                            except WebDriverException as e:
                                                print(e)
                                                actions.send_keys(Keys.F5)
                                            if count_get_next_fb == 9:
                                                driver.quit()
                                                time.sleep(120)
                                                yield scrapy.Request(work_url, self.parse, dont_filter=True)
                                                return
                                            count_get_next_fb += 1
                                    else:
                                        print("[Last Page! No Next Feedback Page Left!]")
                                        break

                        total_rev.append(revenue_total)
                        last_rev.append(revenue_last_month)

                        Product['product_name'] = drug_name[loop_item]
                        Product['product_category'] = drug_class[loop_item]
                        Product['vendor_business_scope'] = vendor_product_quantity[loop_item]
                        Product['product_vendor_url'] = vendor_url[loop_item]
                        Product['product_vendor'] = vendor_name[loop_item]
                        Product['product_left_quantity'] = left_quantity[loop_item]
                        Product['product_price_usd'] = product_price_usd[loop_item]
                        Product['product_sales'] = sales[loop_item]
                        Product['sold_since'] = sold_date[loop_item]
                        Product['vendor_rating'] = rating[loop_item]
                        Product['product_origin'] = origin[loop_item]
                        Product['product_destination'] = destination[loop_item]
                        Product['product_transmit'] = product_trans[loop_item]
                        Product['product_payment_method'] = payment_method[loop_item]
                        Product['product_total_revenue'] = total_rev[loop_item]
                        Product['product_last_month_revenue'] = last_rev[loop_item]
                        Product['total_feedback'] = feedback_total[loop_item]
                        Product['positive_fb'] = feedback_posi[loop_item]
                        Product['negative_fb'] = feedback_nega[loop_item]
                        Product['neutral_fb'] = feedback_neut[loop_item]
                        Product['product_id'] = drug_id[loop_item]

                        yield Product

                        loop_item += 1

                    # crawl next page
                    if next_url is not None:
                        print("[Got Next Page!]")
                        count += 1
                        with open(self.start_page_file, 'w') as file:
                            file.write(str(count))
                        count_get_next = 0
                        while count_get_next < 10:
                            try:
                                old_page = driver.find_element_by_tag_name('html')
                                driver.get(next_url)
                                wait_asc_page = WebDriverWait(driver, 30).until(staleness_of(old_page))
                                time.sleep(2)
                                scrapy_selector = Selector(text=driver.page_source.encode('utf-8'))
                                drug_name_check = scrapy_selector.xpath(
                                    './/div[@class="col-1centre"]/div/a/text()').extract()
                                print("[Drug name check!]")
                                print(len(drug_name_check))
                                if drug_name_check is not None and len(drug_name_check) > 0:
                                    print("[Got next page content!]")
                                    break
                                else:
                                    actions.send_keys(Keys.F5)
                                    print("[Get next page but no content!]")
                            except TimeoutException as te:
                                actions.send_keys(Keys.F5)
                                print(te)
                            except WebDriverException as e:
                                print(e)
                                actions.send_keys(Keys.F5)
                            except Exception as e:
                                actions.send_keys(Keys.F5)
                                print(e)
                                print("[Trouble getting next 15 items]")
                            if count_get_next == 9:
                                driver.quit()
                                time.sleep(60)
                                yield scrapy.Request(self.start_urls[0], self.start_again, dont_filter=True)
                                return
                            count_get_next += 1
                    else:
                        print("[Last {} Page! No Next Page Left!]".format(title_menu[loop_menu]))
                        break
                if count < end_page:
                    driver.quit()
                    time.sleep(60)
                    yield scrapy.Request(work_url, self.parse, dont_filter=True)
                else:
                    loop_menu += 1
                    if loop_menu < len(title_menu):
                        with open(self.start_title, 'w') as file:
                            file.write(str(loop_menu))
                        with open(self.start_page_file, 'w') as file:
                            file.write('1')
                    else:
                        print("[Finish crawling!]")
                        self.get_report()
                        break

    def format_list(self, unformatted_list):
        formatted_list = []
        for i in unformatted_list:
            i = ''.join(i.split())
            formatted_list.append(str(i.encode('ascii', 'ignore')))
        return formatted_list

    def get_report(self):
        start_time = time.time()
        date = str(datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))
        connect = sqlite3.connect(self.workDir + "/product_demo.db")
        with open(self.report, 'a') as f:
            f.write(date + '\n')
        self.empire_market_total_revenue(connect)
        self.get_top_10_vendor_by_revenue(connect)
        self.get_top_10_vendor_by_month_revenue(connect)
        self.get_top_10_product_total_revenue(connect)
        self.get_top_10_product_last_month_revenue(connect)
        self.get_top_10_product_sales_volume(connect)
        self.origins(connect)
        self.destination(connect)
        self.feedback(connect)

    def get_top_10_vendor_by_revenue(self, connect):
        cursor = connect.cursor()
        cursor.execute("""SELECT product_vendor, vendor_rating,
                                COUNT(product_id)  AS product_owned,
                                ROUND(SUM(product_total_revenue), 2)  AS total_revenue,
                                ROUND(SUM(product_last_month_revenue), 2)  AS last_month_revenue
                            FROM product_info_tb
                            GROUP BY product_vendor
                            ORDER BY total_revenue DESC LIMIT 10""")

        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Vendors Ranking by Total Revenue \n")
        for r in result:
            with open(self.report, 'a') as f:
                f.write(
                    "[Vendor]: {}, [Rating]: {}, [Products]: {}, [Total Revenue]: {} USD, [Last Month Revenue]: {} "
                    "USD \n".format(
                        r[0].encode("utf-8"), r[1].encode("utf-8"), r[2], r[3], r[4]))

    def get_top_10_vendor_by_month_revenue(self, connect):
        cursor = connect.cursor()
        cursor.execute("""SELECT product_vendor, vendor_rating,
                                COUNT(product_id)  AS product_owned,
                                ROUND(SUM(product_total_revenue), 2)  AS total_revenue,
                                ROUND(SUM(product_last_month_revenue), 2)  AS last_month_revenue
                            FROM product_info_tb
                            GROUP BY product_vendor
                            ORDER BY last_month_revenue DESC LIMIT 10""")

        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Vendors Ranking by Month Revenue \n")

        for r in result:
            with open(self.report, 'a') as f:
                f.write(
                    "[Vendor]: {}, [Rating]: {}, [Products]: {}, [Total Revenue]: {} USD, [Last Month Revenue]: {} "
                    "USD \n".format(
                        r[0].encode("utf-8"), r[1].encode("utf-8"), r[2], r[3], r[4]))

    def empire_market_total_revenue(self, connect):
        cursor = connect.cursor()
        cursor.execute("SELECT SUM(total_feedback) AS total_sales_volume FROM product_info_tb")
        result5 = cursor.fetchone()
        with open(self.report, 'a') as f:
            f.write("[**** Total Sales Volume]: {} \n".format(result5[0]))
        cursor.execute("""SELECT ROUND(SUM(product_total_revenue), 2) AS total_revenue,
                                    ROUND(SUM(product_last_month_revenue), 2) AS last_month_revenue
                                FROM product_info_tb""")
        result = cursor.fetchall()
        for r in result:
            with open(self.report, 'a') as f:
                f.write("[**** Total Revenue]: {} USD, [Last Month Revenue]: {} USD \n".format(r[0], r[1]))

        cursor.execute("""SELECT DISTINCT product_vendor FROM product_info_tb """)
        result1 = cursor.fetchall()
        number_of_vendor = len(result1)
        with open(self.report, 'a') as f:
            f.write("[**** Total Vendors]: {} \n".format(number_of_vendor))

        cursor.execute("""SELECT DISTINCT * FROM product_info_tb """)
        result2 = cursor.fetchall()
        number_of_product = len(result2)
        with open(self.report, 'a') as f:
            f.write("[**** Total Products]: {} \n".format(number_of_product))

        cursor.execute("DROP TABLE IF EXISTS category_tb2")
        cursor.execute("""CREATE TABLE category_tb2(categories TEXT,
                                                                    count INTEGER,
                                                                    sales INTEGER)""")
        cursor.execute("""SELECT DISTINCT product_category FROM product_info_tb """)
        result3 = cursor.fetchall()
        number_of_category = len(result3)
        with open(self.report, 'a') as f:
            f.write("[**** Total Categories]: {} \n".format(number_of_category))

        with open(self.report, 'a') as f:
            f.write("\n**** Total Categories \n")
        for r in result3:
            t = r
            cursor.execute("SELECT product_id FROM product_info_tb WHERE product_category=?", t)
            result4 = cursor.fetchall()
            number = len(result4)
            cursor.execute("SELECT SUM(total_feedback) AS sales FROM product_info_tb WHERE product_category=?", t)
            result7 = cursor.fetchone()
            cursor.execute("""INSERT INTO category_tb2 VALUES(?,?,?)""", (r[0].encode('utf-8'), number, result7[0]))
            with open(self.report, 'a') as f:
                f.write("[**** Category]: {}, [Quantity of Products]: {}, [Sales Volume]: {} \n".format(
                    r[0].encode("utf-8"), number, result7[0]))

        cursor.execute("SELECT * FROM category_tb2 ORDER BY count DESC LIMIT 15")
        result5 = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 15 Categories Quantity\n")
        for r in result5:
            with open(self.report, 'a') as f:
                f.write("[Category]: {}, [Count]: {} \n".format(r[0].encode('utf-8'), r[1]))

        cursor.execute("SELECT * FROM category_tb2 ORDER BY sales DESC LIMIT 15")
        result6 = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 15 Categories Sales Volume\n")
        for r in result6:
            with open(self.report, 'a') as f:
                f.write("[Category]: {}, [Sales Volume]: {} \n".format(r[0].encode('utf-8'), r[2]))

    def get_top_10_product_total_revenue(self, connect):
        cursor = connect.cursor()
        cursor.execute("""SELECT product_id,
                                product_name,
                                product_category,
                                product_price_usd,
                                product_payment_method,
                                product_sales,
                                product_vendor,
                                product_origin,
                                product_destination,
                                total_feedback,
                                ROUND(product_total_revenue, 2),
                                ROUND(product_last_month_revenue, 2)
                            FROM product_info_tb
                            ORDER BY product_total_revenue DESC LIMIT 10""")
        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Products Ranking by Total Revenue \n")
        for rr in result:
            with open(self.report, 'a') as f:
                f.write("[ID]: {}, [Name]: {}, [Category]: {}, [Price]: {}, [Payment]: {},"
                        " [Sales Volume]: {}, [Vendor]: {}, [Origin]: {}, [Destination]: {},"
                        " [Feedback]: {}, [Total Revenue]: {}, [Last Month Revenue]: {} \n".format(rr[0], rr[1], rr[2],
                                                                                                   rr[3], rr[4], rr[5],
                                                                                                   rr[6], rr[7], rr[8],
                                                                                                   rr[9], rr[10],
                                                                                                   rr[11]))

    def get_top_10_product_last_month_revenue(self, connect):
        cursor = connect.cursor()
        cursor.execute("""SELECT product_id,
                                product_name,
                                product_category,
                                product_price_usd,
                                product_payment_method,
                                product_sales,
                                product_vendor,
                                product_origin,
                                product_destination,
                                total_feedback,
                                ROUND(product_total_revenue, 2),
                                ROUND(product_last_month_revenue, 2)
                            FROM product_info_tb
                            ORDER BY product_last_month_revenue DESC LIMIT 10""")
        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Products Ranking by Last Month Revenue \n")
        for rr in result:
            with open(self.report, 'a') as f:
                f.write("[ID]: {}, [Name]: {}, [Category]: {}, [Price]: {}, [Payment]: {},"
                        " [Sales Volume]: {}, [Vendor]: {}, [Origin]: {}, [Destination]: {},"
                        " [Feedback]: {}, [Total Revenue]: {}, [Last Month Revenue]: {} \n".format(rr[0], rr[1], rr[2],
                                                                                                   rr[3], rr[4], rr[5],
                                                                                                   rr[6], rr[7], rr[8],
                                                                                                   rr[9], rr[10],
                                                                                                   rr[11]))

    def get_top_10_product_sales_volume(self, connect):
        cursor = connect.cursor()
        cursor.execute("""SELECT product_id,
                                product_name,
                                product_category,
                                product_price_usd,
                                product_payment_method,
                                product_sales,
                                product_vendor,
                                product_origin,
                                product_destination,
                                total_feedback,
                                ROUND(product_total_revenue, 2),
                                ROUND(product_last_month_revenue, 2)
                            FROM product_info_tb
                            ORDER BY total_feedback DESC LIMIT 10""")
        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Products Ranking by Sales Volume \n")
        for rr in result:
            with open(self.report, 'a') as f:
                f.write("[ID]: {}, [Name]: {}, [Category]: {}, [Price]: {}, [Payment]: {},"
                        " [Sales Volume]: {}, [Vendor]: {}, [Origin]: {}, [Destination]: {},"
                        " [Feedback]: {}, [Total Revenue]: {}, [Last Month Revenue]: {} \n".format(rr[0], rr[1], rr[2],
                                                                                                   rr[3], rr[4], rr[5],
                                                                                                   rr[6], rr[7], rr[8],
                                                                                                   rr[9], rr[10],
                                                                                                   rr[11]))

    def origins(self, connect):
        cursor = connect.cursor()
        cursor.execute("""DROP TABLE IF EXISTS origin_tb""")
        cursor.execute("""CREATE TABLE origin_tb(origins TEXT,
                                                        count REAL)""")
        cursor.execute("SELECT * FROM product_info_tb")
        total = len(cursor.fetchall())
        cursor.execute("SELECT DISTINCT product_origin FROM product_info_tb")
        origin = cursor.fetchall()
        for o in origin:
            cursor.execute("SELECT * FROM product_info_tb WHERE product_origin=?", o)
            number = len(cursor.fetchall())
            cursor.execute("""INSERT INTO origin_tb VALUES(?,?)""",
                           (o[0].encode('utf-8'), round(float(number) / total, 4)))
        cursor.execute("SELECT * FROM origin_tb ORDER BY count DESC LIMIT 10")
        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Origins \n")
        for r in result:
            with open(self.report, 'a') as f:
                f.write("[Orignin]: {}, [Percentage]: {} \n".format(r[0].encode('utf-8'), r[1]))

    def destination(self, connect):
        cursor = connect.cursor()
        cursor.execute("""DROP TABLE IF EXISTS destination_tb""")
        cursor.execute("""CREATE TABLE destination_tb(origins TEXT,
                                                            count REAL)""")
        cursor.execute("SELECT * FROM product_info_tb")
        total = len(cursor.fetchall())
        cursor.execute("SELECT DISTINCT product_destination FROM product_info_tb")
        destination = cursor.fetchall()
        for o in destination:
            cursor.execute("SELECT * FROM product_info_tb WHERE product_destination=?", o)
            number = len(cursor.fetchall())
            cursor.execute("""INSERT INTO destination_tb VALUES(?,?)""",
                           (o[0].encode('utf-8'), round(float(number) / total, 4)))
        cursor.execute("SELECT * FROM destination_tb ORDER BY count DESC LIMIT 15")
        result = cursor.fetchall()
        with open(self.report, 'a') as f:
            f.write("\nTop 10 Destinations \n")
        for r in result:
            with open(self.report, 'a') as f:
                f.write("[Destination]: {}, [Percentage]: {} \n".format(r[0].encode('utf-8'), r[1]))

    def feedback(self, connect):
        cursor = connect.cursor()
        cursor.execute("SELECT product_sales, total_feedback, positive_fb FROM product_info_tb")
        result = cursor.fetchall()
        total_fdb = 0
        posi_fdb = 0
        sales = 0
        for r in result:
            sales += int(r[0])
            total_fdb += r[1]
            posi_fdb += r[2]
        with open(self.report, 'a') as f:
            f.write("[Sales]: {}, [Total]: {}, [Positive]: {}, [satisfication]: {}".format(sales, total_fdb, posi_fdb,
                                                                                           round(float(
                                                                                               posi_fdb) / total_fdb,
                                                                                                 4)))
