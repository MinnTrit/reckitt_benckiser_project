import save
from save import Saver
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import os
from datetime import datetime
import importlib
import pytz

importlib.reload(save)

#Create the engine

class Processor:
    def __init__(self, input_df, engine):
        self.utc_time = datetime.now(pytz.utc)
        self.vnt_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        self.input_df = input_df
        self.engine = engine
        self.seller_df = None
        self.sku_df = None
        self.sales_df = None

    def create_seller_id(self, row):
        country = row.get('COUNTRY')
        marketplace = row.get('MARKETPLACE')
        seller_id = str(row.get('STORECODE'))
        seller_used_id = '.'.join([country, marketplace, seller_id])
        return seller_used_id
    
    def create_sku_id(self, row):
        country = row.get('COUNTRY', '')
        marketplace = row.get('MARKETPLACE', '')
        seller_id = str(row.get('STORECODE', ''))
        sku_id_marketplace = str(row.get('ITEMCODE', ''))
        sku_used_id = '.'.join([country, marketplace, seller_id, sku_id_marketplace])
        return sku_used_id

    def transform_seller(self):
        seller_columns_list = ['COUNTRY', 'MARKETPLACE', 'STORECODE', 'STOREDESC']
        returned_columns = ['used_id', 'id_marketplace', 'seller_center_code', 'name', 'slug', 'url', 
                            'seller_type', 'token_refresh_latest', 'source', 'created', 'updated',
                            'fk_country_id', 'fk_marketplace_id']
        seller_df = self.input_df.loc[:, seller_columns_list]
        country_query_string = '''
        select id, used_id from user_management_country
        '''
        marketplace_query_string = '''
        select id, used_id from user_management_marketplace
        '''
        seller_df = self.input_df.loc[:, seller_columns_list]
        seller_df.drop_duplicates(subset=seller_columns_list, inplace=True)
        seller_df['used_id'] = seller_df.apply(lambda row: self.create_seller_id(row), axis=1)
        seller_df.rename(columns=
            {'STORECODE': 'id_marketplace',
            'STOREDESC': 'name'
            }, inplace=True)
        with ThreadPoolExecutor(max_workers=2) as executors:
            future1 = executors.submit(self.engine.execute_query, country_query_string, 'retrieve')
            future2 = executors.submit(self.engine.execute_query, marketplace_query_string, 'retrieve')
        country_df = future1.result()
        marketplace_df = future2.result()
        country_map = country_df.set_index('used_id')['id'].to_dict()
        marketplace_map = marketplace_df.set_index('used_id')['id'].to_dict()
        seller_df['fk_country_id'] = seller_df['COUNTRY'].apply(lambda country: country_map.get(country))
        seller_df['fk_marketplace_id'] = seller_df['MARKETPLACE'].apply(lambda marketplace: marketplace_map.get(marketplace))
        seller_df['created'] = self.utc_time.astimezone(self.vnt_timezone)
        seller_df['updated'] = self.utc_time.astimezone(self.vnt_timezone)
        seller_df['source'] = 'minio_upload'
        seller_df['seller_center_code'] = np.nan
        seller_df['slug'] = np.nan
        seller_df['url'] = np.nan
        seller_df['seller_type'] = np.nan
        seller_df['token_refresh_latest'] = np.nan
        returned_df = seller_df.loc[:, returned_columns]
        returned_df.drop_duplicates(inplace=True)
        self.seller_df = returned_df

    def transform_sku(self):
        groupby_columns = ['COUNTRY', 'MARKETPLACE',
        'BUYERCODE', 'VENDORCODE', 'STORECODE', 'STORESHORTCODE', 'STOREDESC', 'BRAND', 
        'ITEMCODE', 'SUPPLIERITEMCODE', 'ITEMDESC', 'SIZE', 'UOM', 'PUF', 'BARCODE']
        sum_metrics = ['SALESAMOUNT', 'SALESQTY', 'INVENTORYONHAND']
        groupby_df = self.input_df.groupby(by=groupby_columns)[sum_metrics].sum().reset_index()
        groupby_df['BARCODE'] = groupby_df['BARCODE'].apply(lambda row:
        int(float(row)) if "+" in row else row)
        #Group of columns extracted from raw
        sku_columns_list = ['COUNTRY', 'MARKETPLACE', 'STORECODE', 'ITEMCODE', 'VENDORCODE', 
        'BARCODE', 'ITEMDESC', 'SALESAMOUNT', 'SALESQTY', 'BRAND']
        #Group of columns to return
        returned_columns = ['brand_raw', 'category_raw', 'spu_used_id', 'spu_id_marketplace', 'url', 'img_url',
                            'spu_id_marketplace_seller', 'used_id', 'barcode', 'name', 'variation_name',
                            'retail_price', 'selling_price', 'fk_seller_id', 'source', 'created', 'updated']
        sku_df = groupby_df.loc[:, sku_columns_list]
        sku_df.drop_duplicates(subset=sku_columns_list, inplace=True)
        sku_df['used_id'] = sku_df.apply(lambda row: self.create_sku_id(row), axis=1)
        sku_df['spu_used_id'] = sku_df.apply(lambda row: self.create_sku_id(row), axis=1)
        sku_df['selling_price'] = round(sku_df['SALESAMOUNT'] / sku_df['SALESQTY'], 2)
        sku_df['retail_price'] = round(sku_df['SALESAMOUNT'] / sku_df['SALESQTY'], 2)
        sku_df.rename(columns=
            {'ITEMCODE': 'spu_id_marketplace',
                'BARCODE': 'barcode',
                'ITEMDESC': 'name',
                'VENDORCODE': 'spu_id_marketplace_seller',
                'BRAND': 'brand_raw'
                }, 
            inplace=True)
        sku_df['barcode'] = sku_df['barcode'].astype(str)
        sku_df['category_raw'] = np.nan
        sku_df['variation_name'] = np.nan
        sku_df['url'] = np.nan
        sku_df['img_url'] = np.nan
        sku_df['source'] = 'minio_upload'
        sku_df['created'] = self.utc_time.astimezone(self.vnt_timezone)
        sku_df['updated'] = self.utc_time.astimezone(self.vnt_timezone)
        #Start getting the fk_seller_id
        sku_df['seller_used_id'] = sku_df['used_id'].apply(lambda used_id:
        '.'.join([used_id.split('.')[0], used_id.split('.')[1], used_id.split('.')[2]]))
        seller_list = sku_df['seller_used_id'].unique().tolist()
        formatted_used_id_list = ", ".join([f"'{used_id}'" for used_id in seller_list])
        query_string = f'''
        select used_id, id
        from ecommerce_seller
        where used_id in (
        {formatted_used_id_list}
        )
        '''
        seller_df = self.engine.execute_query(query_string, 'retrieve')
        seller_map = seller_df.set_index('used_id')['id'].to_dict()
        sku_df['fk_seller_id'] = sku_df['seller_used_id'].apply(lambda used_id:
        seller_map.get(used_id))
        returned_df = sku_df.loc[:, returned_columns]
        returned_df.drop_duplicates(inplace=True)
        returned_df.replace("'", "", regex=True, inplace=True)
        self.sku_df = returned_df 

    def transform_sales(self):
        country_query_string = '''
        select used_id, from_usd_xrate from user_management_country
        '''
        country_df = self.engine.execute_query(country_query_string, 'retrieve')
        xrate_map = country_df.set_index('used_id')['from_usd_xrate'].to_dict()
        groupby_columns = ['MARKETPLACE', 'COUNTRY', 'TRXDATE', 'BUYERCODE', 'VENDORCODE', 'STORECODE', 'STORESHORTCODE', 'STOREDESC', 'BRAND', 'ITEMCODE', 'SUPPLIERITEMCODE', 'ITEMDESC', 'SIZE', 'UOM', 'PUF', 'BARCODE']
        sum_metrics = ['SALESAMOUNT', 'SALESQTY']
        returned_columns = ['day', 'fk_company_used_id', 'fk_sku_used_id', 'order_count', 'quantity', 's_retail', 'retail_price', 
                            's_onsite_selling', 'onsite_selling_price', 's_retail_company', 'retail_price_company', 
                            's_seller_selling_company', 'seller_selling_price_company', 's_onsite_selling_company', 
                            'onsite_selling_price_company', 'created', 'updated', 'fk_status_used_id', 'source']
        groupby_df = self.input_df.groupby(by=groupby_columns)[sum_metrics].sum().reset_index()
        groupby_df['fk_sku_used_id'] = groupby_df.apply(lambda row: self.create_sku_id(row), axis=1)
        groupby_df['fk_status_used_id'] = groupby_df['SALESQTY'].apply(lambda quantity: 
        'delivered' if quantity > 0 else 'canceled')
        groupby_df['SALESQTY'] = groupby_df['SALESQTY'].abs()
        groupby_df['SALESAMOUNT'] = groupby_df['SALESAMOUNT'].abs()
        groupby_df['order_count'] = 0
        groupby_df['xrate'] = pd.to_numeric(groupby_df['COUNTRY'].apply(lambda country: xrate_map.get(country)))
        groupby_df['SALESAMOUNT'] = groupby_df['SALESAMOUNT'] * groupby_df['xrate']
        groupby_df['onsite_selling_price'] = groupby_df['SALESAMOUNT'] / groupby_df['SALESQTY']
        groupby_df['retail_price'] = groupby_df['SALESAMOUNT'] / groupby_df['SALESQTY']
        groupby_df['seller_selling_price_company'] = groupby_df['SALESAMOUNT'] / groupby_df['SALESQTY']
        groupby_df['retail_price_company'] = groupby_df['SALESAMOUNT'] / groupby_df['SALESQTY']
        groupby_df['onsite_selling_price_company'] = groupby_df['SALESAMOUNT'] / groupby_df['SALESQTY']
        groupby_df['s_retail'] = groupby_df['SALESAMOUNT']
        groupby_df['s_onsite_selling'] = groupby_df['SALESAMOUNT']
        groupby_df['s_retail_company'] = groupby_df['SALESAMOUNT']
        groupby_df['s_seller_selling_company'] = groupby_df['SALESAMOUNT']
        groupby_df['s_onsite_selling_company'] = groupby_df['SALESAMOUNT']
        groupby_df['updated'] = self.utc_time.astimezone(self.vnt_timezone)
        groupby_df['created'] = self.utc_time.astimezone(self.vnt_timezone)
        groupby_df['source'] = 'minio_upload'
        groupby_df['fk_company_used_id'] = 'reckitt_benckiser_offline'
        groupby_df.rename(columns={
            'TRXDATE': 'day',
            'SALESQTY': 'quantity'
        },inplace=True)
        returned_df = groupby_df.loc[:, returned_columns]
        returned_df.replace(float('inf'), 0, inplace=True)
        self.sales_df = returned_df

    def save_seller(self):
        self.engine.to_database_seller(self.seller_df, 'ecommerce_seller')

    def save_sku(self):
        self.engine.to_database_sku(self.sku_df, 'ecommerce_sku')

    def save_sales(self):
        self.engine.to_database_sales(self.sales_df, 'ecommerce_export_sku_sales')

if __name__ == '__main__':
    pass