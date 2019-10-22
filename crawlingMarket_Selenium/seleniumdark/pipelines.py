# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3


class SeleniumdarkPipeline(object):

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.connection = sqlite3.connect("product_demo_10092019.db")
        self.cursor = self.connection.cursor()

    def create_table(self):
        # self.cursor.execute("""DROP TABLE IF EXISTS product_info_tb""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS product_info_tb(
                                    product_id TEXT,
                                    product_name TEXT,
                                    product_category TEXT,
                                    product_price_usd TEXT,
                                    product_sales TEXT,
                                    sold_since TEXT,
                                    product_left_quantity TEXT,
                                    product_origin TEXT,
                                    product_destination TEXT,
                                    product_transmit TEXT,
                                    product_payment_method TEXT,
                                    product_total_revenue REAL,
                                    product_last_month_revenue REAL,
                                    product_vendor TEXT,
                                    product_vendor_url TEXT,
                                    vendor_rating TEXT,
                                    vendor_business_scope TEXT,
                                    total_feedback INTEGER,
                                    positive_fb INTEGER,
                                    negative_fb INTEGER,
                                    neutral_fb INTEGER
                                    )""")

    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        self.cursor.execute("""INSERT INTO product_info_tb VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            item['product_id'],
            item['product_name'],
            item['product_category'],
            item['product_price_usd'],
            item['product_sales'],
            item['sold_since'],
            item['product_left_quantity'],
            item['product_origin'],
            item['product_destination'],
            item['product_transmit'],
            item['product_payment_method'],
            item['product_total_revenue'],
            item['product_last_month_revenue'],
            item['product_vendor'],
            item['product_vendor_url'],
            item['vendor_rating'],
            item['vendor_business_scope'],
            item['total_feedback'],
            item['positive_fb'],
            item['negative_fb'],
            item['neutral_fb']
        ))
        self.connection.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
