#!/usr/bin/env python
# encoding: utf-8

"""
getTransactions.py

1. Log into Systopia
2. Paginate through a list of transactions
3. For each transaction, get details
4. Write to database
"""

import requests
#import requests_cache
#requests_cache.install_cache('canteen_cache')

import MySQLdb
from upsert import upsert

from lxml import html, etree
import re

db = MySQLdb.connect(host="localhost", user="root", passwd="", db="canteen", charset="utf8")

summary_url_template = "https://newsbuilding.systopiacloud.com/History/Transactions?page={page}"
detail_url_template = "https://newsbuilding.systopiacloud.com/History/Transaction?transId={transaction_id}"
jar = requests.cookies.RequestsCookieJar()
jar.set('__RequestVerificationToken','OOZbRfZYcVH_y354bIRRdL9SGdSLZ9kiLTPQIn3zDcvf7CRiJQBy1xsdFBq3J5P9I47kr5N7plmKS7qWa2KwRu6ZyyKBR9aLcIDIb0W6krk1')
jar.set('.AspNet.ApplicationCookie','TOGgqRRcdbnT38p4tuWvBt84NC2lJaEZvpHrrUZcTgu-_A5gikHph-JpFUa1OU8aGO8vwCr9ayLIYCBCA6udXSp5vu9p37PFX-leBXpxrSRijaP8ax_WFVWoVQ_WAv1Gr8-i8dD-lx9Kf-b0uLnGd2g44IVHNEUGF-xij6hZaPjYXjCsyp0pSq8tgcfChEvm3ChR4fn3n9gWStC-LTw5xLWymg50AmeqdYr1Z_ZvzO39ef5POZ8pgTQ-IUezdk6zkJ-dlMtcKYcsOMzgH2s_kegnG-j9tmkzimzSvEms9y3mfj31tb2M6dDPffcZb8dAUNDpus5xYGL4ZaSLt02cKN7BL3qpkFLeM1TtJYkiI4ytz0BAWhGrp1RfSabe9EiOrpVKLOBUkzrf90A5PSeYeR2QElDfzk0rAH3eSG5-yVJs9T_RUvFSEmnWf5j53SW_TtEr3ULs68Bi7_4weubRCQ')

field_handlers = {
    'Details':'./button/@onclick'
}

db_transaction_fields = [
    "id",
    "date",
    "type",
    "amount",
    "location",
    "balance"
]

db_item_fields = [
    "sequence_id",
    "transaction_id",
    "name",
    "price"
]

def processRow(row):
    for field,element in row.iteritems():
        if field == 'Details':
            xpath_string = './button/@onclick'
        else:
            xpath_string = './text()'

        row[field] = element.xpath(xpath_string)[0].replace(u'\xa3','')

        if field == 'Details':
            row[field] = re.search(r'loadDetailedTransaction\((\d*)\)',row[field]).group(1)
    return row

def getTransactions(starting_page = 1, ending_page = 10):
    while True:
        scrape_doc = requests.get(
            summary_url_template.format(page=starting_page),cookies=jar
            )
        scrape_tree = html.fromstring(scrape_doc.text)
        head_elements = scrape_tree.xpath('//thead/tr/th')
        fields = [e.xpath('./text()')[0] for e in head_elements]
        print fields
        data_rows = scrape_tree.xpath('//tbody/tr')
        list_rows = []
        for row in data_rows:
            dict_row = dict(zip(fields,row))
            dict_row = processRow(dict_row)
            date = '-'.join(reversed(dict_row['Date'].split('/')))
            print dict_row['Date'], date
            list_row = [
                dict_row['Details'],
                date + ' ' + dict_row['Time'],
                dict_row['Type'],
                dict_row['Amount'],
                dict_row['Location'],
                dict_row['Balance']
            ]
            print list_row
            list_rows.append(list_row)
        upsert(db, 'transactions', db_transaction_fields, list_rows)
        starting_page += 1
        if len(data_rows) == 0 or starting_page > ending_page:
            break

def getDetails():
    #get transaction ids without detail flag set
    cursor = db.cursor()
    cursor.execute("""SELECT id FROM transactions WHERE moneyFormat IS NULL""")
    #for each of these, request the detail page
    for row in cursor:
        print row
        transaction_id = row[0]
        scrape_doc = requests.get(detail_url_template.format(transaction_id=transaction_id),cookies=jar)
        #parse the detail page into records for the item table
        scrape_tree = html.fromstring(scrape_doc.text)
        #https://stackoverflow.com/a/14295136
        items = scrape_tree.xpath('(//tbody)[1]/tr')
        db_items_rows = []
        for sequence_id, item in enumerate(items):
            item_elements = item.xpath('./td')
            name = item_elements[0].xpath('./text()')[0]
            price = item_elements[1].xpath('./text()')[0].replace(u'\xa3','')
            db_items_rows.append([sequence_id, transaction_id, name, price])
        #save these items
        upsert(db, 'items', db_item_fields, db_items_rows)
        money_format_item = scrape_tree.xpath('(//tbody)[2]/tr/td')
        money_format = money_format_item[0].xpath('./text()')[0]
        #update the transaction table detail flag
        upsert(db, 'transactions', ['id','moneyFormat'], [[transaction_id,money_format]])

getTransactions()
getDetails()
