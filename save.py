from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import re
import os

class Saver:
    username = os.getenv('username')
    password = os.getenv('password')
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        self.engine = self.initialize_engine()

    def initialize_engine(self):
        connection_url = f'mysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        engine = create_engine(connection_url)
        return engine

    def get_information(self):
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database
        }
    
    def to_database_seller(self, input_df, table_name):
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        current_list = input_df['used_id'].values.tolist()
        formatted_used_id_list = ", ".join([f"'{used_id}'" for used_id in current_list])
        if table_name in table_list: 
            update_queries = []
            insert_queries = []
            query = f"SELECT used_id, id from ecommerce_seller WHERE used_id IN ({formatted_used_id_list})"
            validate_used_id_df = self.execute_query(query, 'retrieve')
            if len(validate_used_id_df) > 0:
                validate_used_id_list = validate_used_id_df['used_id'].values.tolist()
                existed_df = input_df.loc[input_df['used_id'].isin(validate_used_id_list), :]
                nonexisted_df = input_df.loc[~input_df['used_id'].isin(validate_used_id_list), :]
                for index, row in existed_df.iterrows():
                    seller_center_code = 'NULL' if pd.isna(row['seller_center_code']) else f"'{row['seller_center_code']}'"
                    token_refresh_latest = 'NULL' if pd.isna(row['token_refresh_latest']) else f"'{row['token_refresh_latest']}'"
                    slug = 'NULL' if pd.isna(row['slug']) else f"'{row['slug']}'"
                    url = 'NULL' if pd.isna(row['url']) else f"'{row['url']}'"
                    seller_type = 'NULL' if pd.isna(row['seller_type']) else f"'{row['seller_type']}'"
                    query = f"""
                    UPDATE {table_name}
                    SET 
                        id_marketplace = {row['id_marketplace']},
                        seller_center_code = {seller_center_code},
                        name = '{row['name']}',
                        slug = {slug},
                        url = {url},
                        seller_type = {seller_type},
                        token_refresh_latest = {token_refresh_latest},
                        source = '{row['source']}',
                        updated = '{row['updated']}',
                        fk_country_id = {row['fk_country_id']},
                        fk_marketplace_id = {row['fk_marketplace_id']}
                    WHERE used_id = '{row['used_id']}'
                    """
                    update_queries.append(query)
                for index, row in nonexisted_df.iterrows():
                    seller_center_code = 'NULL' if pd.isna(row['seller_center_code']) else f"'{row['seller_center_code']}'"
                    token_refresh_latest = 'NULL' if pd.isna(row['token_refresh_latest']) else f"'{row['token_refresh_latest']}'"
                    slug = 'NULL' if pd.isna(row['slug']) else f"'{row['slug']}'"
                    url = 'NULL' if pd.isna(row['url']) else f"'{row['url']}'"
                    seller_type = 'NULL' if pd.isna(row['seller_type']) else f"'{row['seller_type']}'"
                    query = f"""
                    INSERT INTO {table_name} (
                        id_marketplace,
                        seller_center_code,
                        name,
                        slug,
                        url,
                        seller_type,
                        token_refresh_latest,
                        source,
                        created,
                        updated,
                        fk_country_id,
                        fk_marketplace_id,
                        used_id 
                    )
                    VALUES (
                        {row['id_marketplace']},
                        {seller_center_code},
                        '{row['name']}',
                        {slug},
                        {url},
                        {seller_type},
                        {token_refresh_latest},
                        '{row['source']}',
                        '{row['created']}',
                        '{row['updated']}',
                        {row['fk_country_id']},
                        {row['fk_marketplace_id']},
                        {row['used_id']}
                    )
                    """
                    insert_queries.append(query)
                combined_update_query = ";\n".join(update_queries)
                combined_insert_query = ";\n".join(insert_queries)
                with ThreadPoolExecutor(max_workers=2) as executors:
                    if len(combined_update_query) > 0:
                        executors.submit(self.execute_query, combined_update_query, 'update')
                    if len(combined_insert_query) > 0:
                        executors.submit(self.execute_query, combined_insert_query, 'update')
                print(f'Saved table {table_name} to database {self.database}')  
            else:
                input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False) 
                print(f'Saved table {table_name} to database {self.database}')

    def to_database_sku(self, input_df, table_name):
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        current_list = input_df['used_id'].values.tolist()
        formatted_used_id_list = ", ".join([f"'{used_id}'" for used_id in current_list])
        if table_name in table_list: 
            update_queries = []
            insert_queries = []
            string_query = f"SELECT used_id, id from ecommerce_sku where used_id IN ({formatted_used_id_list})"
            validate_used_id_df = self.execute_query(string_query, 'retrieve')
            if len(validate_used_id_df) > 0:
                validate_used_id_list = validate_used_id_df['used_id'].values.tolist()
                existed_df = input_df.loc[input_df['used_id'].isin(validate_used_id_list), :]
                nonexisted_df = input_df.loc[~input_df['used_id'].isin(validate_used_id_list), :]
                for index, row in existed_df.iterrows():
                    category_raw = 'NULL' if pd.isna(row['category_raw']) else f"'{row['category_raw']}'"
                    brand_raw = 'NULL' if pd.isna(row['brand_raw']) else f"'{row['brand_raw']}'"
                    barcode = 'NULL' if pd.isna(row['barcode']) else f"'{row['barcode']}'"
                    variation_name = 'NULL' if pd.isna(row['variation_name']) else f"'{row['variation_name']}'"
                    img_url = 'NULL' if pd.isna(row['img_url']) else f"'{row['img_url']}'"
                    url = 'NULL' if pd.isna(row['url']) else f"'{row['url']}'"
                    query = f"""
                    UPDATE {table_name}
                    SET 
                        category_raw = {category_raw},
                        brand_raw = {brand_raw},
                        variation_name = {variation_name},
                        img_url = {img_url},
                        url = {url},
                        spu_used_id = '{row['spu_used_id']}',
                        spu_id_marketplace = '{row['spu_id_marketplace']}',
                        spu_id_marketplace_seller = '{row['spu_id_marketplace_seller']}',
                        used_id = '{row['used_id']}',
                        barcode = {barcode},
                        name = '{row['name']}',
                        retail_price = {row['retail_price']},
                        selling_price = {row['selling_price']},
                        source = '{row['source']}',
                        updated = '{row['updated']}',
                        fk_seller_id = {row['fk_seller_id']}
                    WHERE used_id = '{row['used_id']}'
                    """
                    update_queries.append(query)
                for index, row in nonexisted_df.iterrows():
                    category_raw = 'NULL' if pd.isna(row['category_raw']) else f"'{row['category_raw']}'"
                    brand_raw = 'NULL' if pd.isna(row['brand_raw']) else f"'{row['brand_raw']}'"
                    barcode = 'NULL' if pd.isna(row['barcode']) else f"'{row['barcode']}'"
                    variation_name = 'NULL' if pd.isna(row['variation_name']) else f"'{row['variation_name']}'"
                    img_url = 'NULL' if pd.isna(row['img_url']) else f"'{row['img_url']}'"
                    url = 'NULL' if pd.isna(row['url']) else f"'{row['url']}'"
                    query = f"""
                    INSERT INTO {table_name} (
                        category_raw,
                        brand_raw,
                        variation_name,
                        img_url,
                        url,
                        spu_used_id,
                        spu_id_marketplace,
                        spu_id_marketplace_seller,
                        used_id,
                        barcode,
                        name,
                        retail_price,
                        selling_price,
                        source',
                        updated,
                        created,
                        fk_seller_id
                    )
                    VALUES (
                        {category_raw},
                        {brand_raw},
                        {variation_name},
                        {img_url},
                        {url},
                        '{row['spu_used_id']}',
                        '{row['spu_id_marketplace']}',
                        '{row['spu_id_marketplace_seller']}',
                        '{row['used_id']}',
                        '{barcode}',
                        '{row['name']}',
                        '{row['retail_price']}',
                        '{row['selling_price']}',
                        '{row['source']}',
                        '{row['updated']}',
                        '{row['created']}',
                        '{row['fk_seller_id']}'
                    )
                    """
                    insert_queries.append(query)
                with ThreadPoolExecutor(max_workers=4) as executors:
                    if len(update_queries) > 0:
                        for i in range(0, len(update_queries), 4000):
                            batch = ";\n".join(update_queries[i:i + 4000])
                            executors.submit(self.execute_query, batch, 'update')
                    if len(insert_queries) > 0:
                        for i in range(0, len(insert_queries), 4000):
                            batch = ";\n".join(insert_queries[i:i + 4000])
                            executors.submit(self.execute_query, batch, 'update')
                print(f'Saved table {table_name} to database {self.database}')  
            else:
                input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
                print(f'Saved table {table_name} to database {self.database}')

    def to_database_sales(self, input_df, table_name):
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        if table_name in table_list:
            input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False) 
            print(f'Saved table {table_name} to database {self.database}')  
        else: 
            print(f'Table {table_name} does not existed in database {self.database}')
  
    def execute_query(self,
                      string_query:str,
                      query_type:list[str]=['retrieve', 'update']):
        while True:
            try:
                query = text(string_query)
                with self.engine.connect() as connection:
                    print('Database connection created')
                    if query_type == 'retrieve':
                        matching_pattern = r'from\s+(\w+)(?:\s+)?.*'
                        table_name = re.search(matching_pattern, string_query).group(1)
                        rows = connection.execute(query)
                        results = rows.fetchall()
                        print(f'Fetched {len(results)} rows from table {table_name}')
                        return pd.DataFrame(results)
                    else:
                        matching_pattern = r'(?:INSERT INTO|insert into|UPDATE|update)\s+(.*)(?:\s*)?'
                        table_name = re.search(matching_pattern, string_query).group(1)
                        connection.execute(query)
                        connection.commit()
                        print(f'Updated data for table {table_name}')
                connection.close()
                break
            except Exception as e:
                with open('error.txt', 'w') as file:
                    file.write(str(e))
                print(f'Error occured during the query: {e}')
                time.sleep(10)
                continue

    def close_engine(self):
        self.engine.dispose()
        print(f'Closed engine connection to the database')

if __name__ == '__main__':
    pass

    

    