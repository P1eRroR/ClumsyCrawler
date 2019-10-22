# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CategoryItem(scrapy.Item):
    main_menu_title = scrapy.Field()
    main_menu_url = scrapy.Field()
    main_menu_quantity = scrapy.Field()


class ProductItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    product_category = scrapy.Field()  #
    product_name = scrapy.Field()  #
    product_price_usd = scrapy.Field()  #
    product_id = scrapy.Field()
    # product_price_btc = scrapy.Field()
    product_sales = scrapy.Field()  #
    sold_since = scrapy.Field()
    product_left_quantity = scrapy.Field()  #
    product_origin = scrapy.Field()  #
    product_destination = scrapy.Field()  #
    product_transmit = scrapy.Field()  #
    product_payment_method = scrapy.Field()  #
    product_total_revenue = scrapy.Field()
    product_last_month_revenue = scrapy.Field()  # ?
    product_vendor = scrapy.Field()  #
    product_vendor_url = scrapy.Field()  #
    vendor_rating = scrapy.Field()
    vendor_business_scope = scrapy.Field()
    total_feedback = scrapy.Field()
    positive_fb = scrapy.Field()
    negative_fb = scrapy.Field()
    neutral_fb = scrapy.Field()
    # latest_review_time = scrapy.Field()


class VendorItem(scrapy.Item):
    vendor_name = scrapy.Field()  #
    vendor_url = scrapy.Field()  #
    # vendor_ship_from = scrapy.Field()
    # vendor_ship_to = scrapy.Field()
    vendor_active_period = scrapy.Field()
    vendor_rank = scrapy.Field()
    vendor_business_scope = scrapy.Field()  #


# class FeedbackItem(scrapy.Item):
#     product_name = scrapy.Field()
#     total = scrapy.Field()
#     positive = scrapy.Field()
#     negative = scrapy.Field()
#     neutral = scrapy.Field()
#     review_time = scrapy.Field()
