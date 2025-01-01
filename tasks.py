from save import Saver
from celery import Celery
from processing import Processor
import pandas as pd
import os
broker_url = os.getenv('broker_url')
app = Celery('tasks', broker=broker_url)

@app.task
def main_task(file_path):
    print(f'Received file path {file_path}')
    host = os.getenv('instance_host')
    port = '3306'
    database = 'dw_reckitt_benckiser_offline'
    filename = os.path.basename(file_path)
    marketplace = filename.split("_")[0]
    raw_df = pd.read_csv(file_path)
    raw_df['TRXDATE'] = pd.to_datetime(raw_df['TRXDATE'])
    raw_df['MARKETPLACE'] = marketplace
    raw_df['COUNTRY'] = 'SG'
    engine = Saver(host, port, database)
    processor = Processor(raw_df, engine)
    processor.transform_seller()
    processor.save_seller()
    processor.transform_sku()
    processor.save_sku()
    processor.transform_sales()
    processor.save_sales()
    engine.close_engine()