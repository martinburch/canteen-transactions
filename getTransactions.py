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
import requests_cache
requests_cache.install_cache('canteen_cache')

import MySQLdb
from upsert import upsert

from lxml import html, etree
import re

db = MySQLdb.connect(host="localhost", user="root", passwd="", db="canteen", charset="utf8")

summary_url_template = "https://newsbuilding.systopiacloud.com/History/Transactions?page={page}"
detail_url_template = "https://newsbuilding.systopiacloud.com/History/Transaction?transId={ttransaction_id}"
starting_page = 1
jar = requests.cookies.RequestsCookieJar()
jar.set('__RequestVerificationToken','9BccjDQ4mWQUNYtu0A5j2iC_DLYczXW-VqqPdy84ylvMwb8P0Diesis1XebrqrcncnbEoo-54buQ5W6f9wdauayh1XuSWKyS7_B8JuzcfAk1')
jar.set('.AspNet.ApplicationCookie','_AxkViGPipruK3_TSOxZpFIwwVkN8LASncoZOCXYkOMJ-McC15Qd9g0JUxFt_upoQIFvt8O8QjKSj800McRtyJBm1G-6BUac0Gb0EaAm9Rv5ZKApdikUlJ0XYjPi__K-ZiGIxd1rRbGz45tpoUWPYlTu4OnFf_OlPOOE9e891jENH1QkBgVhLL5vNTCMGuzgdaymh8c7gXCgeNaF3HgZ8PwaAPrXftqa0YtvgCgIXnkXCpKWMdB--nR68C-FdubiZ29h17KrZBPV1x8HiQzMYNo88W6RLTN67BUX3aBbCXtf4iI2l4qPT2OvrebdAoIsKzKTRU1YTyKI5wclwpOZvmxoX9b4YGnMsjzEcSadkGALxXpn2S6pJ0VQ--5fNIcooUU0Cw9sEFypmdZWmncEW2t84kZMrRsSSg84mJq9ikH4rdIJotXLjV0R9GpC-UVSqUmsqPCH-0VApVVqJ3FIMg')

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

def getTransactions():
    while True:
        scrape_doc = requests.get(
            summary_url_template.format(page=starting_page),cookies=jar
            )
        print scrape_doc.text
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
        if len(data_rows) == 0:
            break

def getDetails():
    #get transaction ids without detail flag set
    cursor = db.cursor()
    cursor.execute("""SELECT id FROM transactions WHERE detailLoaded IS FALSE""")
    #for each of these, request the detail page
    for row in cursor:
        transaction_id = row[0]
        #scrape_doc = requests.get(detail_url_template.format(transaction_id=transaction_id),cookies=jar)
        #parse the detail page into records for the item table
        #save these items
        #update the transaction table detail flag

getDetails()
