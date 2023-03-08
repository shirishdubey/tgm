#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 01:16:26 2023

@author: shirish.dubey
"""

import pandas as pd
from sqlalchemy import create_engine
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--input_source_username', required=True, type=str, help='DB user name')
parser.add_argument('--input_source_password', required=True, type=str, help='DB password')

parser = argparse.ArgumentParser()
parser.add_argument('--input_staging_username', required=True, type=str, help='DB user name')
parser.add_argument('--input_staging_password', required=True, type=str, help='DB password')

namespace = parser.parse_args()


input_source_username = namespace.input_source_username
input_source_password = namespace.input_source_password

input_staging_username = namespace.input_staging_username
input_staging_password = namespace.input_staging_password

dbusername1 = input_source_username
dbpassword1 = input_source_password
dbip1 = '192.168.XX.XXX' 
dbport1 = 'XXXXX' 
dbschema1 = 'DBSCHEMA'

dbusername2 = input_staging_username
dbpassword2 = input_staging_password
dbip2 = '192.168.XX.XXX' 
dbport2 = 'XXXXX' 
dbschema2 = 'DBSCHEMA'


source_engine = create_engine('hana+hdbcli://{}:{}@{}:{}'.format(dbusername1, dbpassword1, dbip1, dbport1))
source_connection = source_engine.connect()

staging_engine = create_engine('hana+hdbcli://{}:{}@{}:{}'.format(dbusername2, dbpassword2, dbip2, dbport2))
staging_connection = staging_engine.connect()

# change data capture in orders from app
df_orders_app_source = pd.DataFrame(source_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_app" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
  

df_orders_app_staging = pd.DataFrame(staging_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_app" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
                             
df_orders_app_merged = ( df_orders_app_staging.merge(df_orders_app_source,how='outer',sort=False)
                  .drop_duplicates(['pushed_to_staging_on'],keep='last')
                  .sort_values('pushed_to_staging_on')
                  .reset_index(drop=True) )                             
                             
                             
print("uploading orders_app table")
df_orders_app_merged.to_sql('staging.orders_app', con = staging_connection,schema = dbschema2, if_exists = 'append', index = False)


# change data capture in orders from pos
df_orders_pos_source = pd.DataFrame(source_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_pos" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
  

df_orders_pos_staging = pd.DataFrame(staging_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_pos" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
                             
df_orders_pos_merged = ( df_orders_pos_staging.merge(df_orders_pos_source,how='outer',sort=False)
                  .drop_duplicates(['pushed_to_staging_on'],keep='last')
                  .sort_values('pushed_to_staging_on')
                  .reset_index(drop=True) )                             
                             
                             
print("uploading orders_pos table")
df_orders_pos_merged.to_sql('staging.orders_pos', con = staging_connection,schema = dbschema2, if_exists = 'append', index = False)


# change data capture in orders from ecomm
df_orders_ecomm_source = pd.DataFrame(source_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_ecomm" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
  

df_orders_ecomm_staging = pd.DataFrame(staging_engine.execute(''' SELECT "id", "customer_id", "date", "total", "pushed_to_staging_on"
                                 FROM "app"."orders_ecomm" ''').fetchall(),
                                 columns=["id",
                                 "customer_id",
                                 "date",
                                 "total",
                                 "pushed_to_staging_on"])
                             
df_orders_ecomm_merged = ( df_orders_ecomm_staging.merge(df_orders_ecomm_source,how='outer',sort=False)
                  .drop_duplicates(['pushed_to_staging_on'],keep='last')
                  .sort_values('pushed_to_staging_on')
                  .reset_index(drop=True) )                             
                             
                             
print("uploading orders_ecomm table")
df_orders_pos_merged.to_sql('staging.orders_ecomm', con = staging_connection,schema = dbschema2, if_exists = 'append', index = False)






