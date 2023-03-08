#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 00:09:33 2023

@author: shirish.dubey
"""

import pandas as pd
from sqlalchemy import create_engine
import argparse
from datetime import datetime



parser = argparse.ArgumentParser()
parser.add_argument('--input_source_username', required=True, type=str, help='DB user name')
parser.add_argument('--input_source_password', required=True, type=str, help='DB password')

namespace = parser.parse_args()

input_source_username = namespace.input_source_username
input_source_password = namespace.input_source_password

dbusername = input_source_username
dbpassword1 = input_source_password
dbip = '192.168.XX.XXX' 
dbport = 'XXXXX' 
dbschema = 'DBSCHEMA'


engine = create_engine('hana+hdbcli://{}:{}@{}:{}'.format(dbusername, dbpassword1, dbip, dbport))
connection = engine.connect()

# loading dw's staging area with orders from app
df_orders_app = pd.DataFrame(engine.execute(''' SELECT "id", "customer_id", "date", "total"
                                 FROM "app"."orders_app" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total"])

df_orders_app['pushed_to_staging_on']= datetime.today().strftime('%d-%b-%y %H:%M:%S')
                             
print("uploading orders_app table")
df_orders_app.to_sql('staging.orders_app', con = connection,schema = dbschema, if_exists = 'append', index = False)

# loading dw's staging area with orders from pos
df_orders_pos = pd.DataFrame(engine.execute(''' SELECT "id", "customer_id", "date", "total"
                                 FROM "app"."orders_pos" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total"])

df_orders_pos['pushed_to_staging_on']= datetime.today().strftime('%d-%b-%y %H:%M:%S')
                             
print("uploading orders_pos table")
df_orders_pos.to_sql('staging.orders_pos', con = connection,schema = dbschema, if_exists = 'append', index = False)
      
      # loading dw's staging area with orders from ecomm                           
df_orders_ecomm = pd.DataFrame(engine.execute(''' SELECT "id", "customer_id", "date", "total"
                                 FROM "app"."orders_ecomm" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total"])
  
df_orders_ecomm['pushed_to_staging_on']= datetime.today().strftime('%d-%b-%y %H:%M:%S')

print("uploading orders_ecomm table")
df_orders_ecomm.to_sql('staging.orders_ecomm', con = connection,schema = dbschema, if_exists = 'append', index = False)
