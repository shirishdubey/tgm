#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 00:35:53 2023

@author: shirish.dubey
"""
import pandas as pd
from sqlalchemy import create_engine
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--input_staging_username', required=True, type=str, help='DB user name')
parser.add_argument('--input_staging_password', required=True, type=str, help='DB password')

namespace = parser.parse_args()

input_staging_username = namespace.input_staging_username
input_staging_password = namespace.input_staging_password

dbusername = input_staging_username
dbpassword1 = input_staging_password
dbip = '192.168.XX.XXX' 
dbport = 'XXXXX' 
dbschema = 'DBSCHEMA'


engine = create_engine('hana+hdbcli://{}:{}@{}:{}'.format(dbusername, dbpassword1, dbip, dbport))
connection = engine.connect()

# loading dw's cdm with orders from all order related tables from staging area
df_orders = pd.DataFrame(engine.execute(''' SELECT id, customer_id,
                                TO_DATE(date, 'YYYY-MM-DD’) as date, total FROM orders_pos)
                                UNION
                                (SELECT id, customer_id,
                                TO_DATE(date, 'YYYY-MM-DD’) as date, total FROM orders_app)
                                UNION
                                (SELECT id, customer_id,
                                TO_DATE(date, 'YYYY-MM-DD’) as date, total FROM orders_ecomm ''').fetchall(),
                                columns=["id",
                                         "customer_id",
                                         "date",
                                         "total"])

print("uploading orders table")
df_orders.to_sql('staging.orders_pos', con = connection,schema = dbschema, if_exists = 'append', index = False)