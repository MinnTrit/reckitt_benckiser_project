# Sample Video
https://github.com/user-attachments/assets/7bdf0691-dc3e-4cd8-b6ef-ccf9b63a37d4

# Overall Diagram
![image](https://github.com/user-attachments/assets/92e30e8e-fbde-405e-852a-dc0fedbdd673)

# Overall Data Model
![image](https://github.com/user-attachments/assets/c202b96d-edaf-4d0f-a166-9c2b17a5acc5)

# Diagram breakdown
1. ```Users``` upload file to ```Flask``` server: The user initialize the task by uploading csv/xlsx file in internal format, the file must have the prefix of the marketplace stored in the database (For ex: ```GDN_``` for Guardian, ```SEV_``` for 7Eleven)
2. ```Flask``` server receives the uploaded file and enqueue the job on ```redis message broker```: Flask will read the file from the folder ```uploadeds``` and put this file along with the celery tasks on waiting line of the message broker
3. ```Celery``` service listening on the server and enqueue the message: Since we have the celery worker running as the ```celery_worker.service```, it will always running as the background job on the server, hence, it can receive incoming traffic sent on the ```redis-server``` and execute the tasks, sub-tasks involved:
   * Class ```Processor```: Mainly in charge of transforming the data based on company's internal logic and dimension mapping to the database
   * Class ```Saver```: Mainly in charge of defining logic for inserting/updating data to the right table
4. ```Master``` database gets updated from ```celery``` worker on host ```http://129.151.180.13```: Celery worker will use the ```master_account``` with privileges accesses to all tables to insert the data to 3 main tables, including:
   * ```ecommerce_sku```: Stores SKUs' informations
   * ```ecommerce_seller```: Stores sellers' informations
   * ```ecommerce_export_sku_sales```: Stores sales performance from marketplaces
=> For users' accesses, it will be granted as ```select on``` access (Read only)
5. ```Slave``` database gets updated from ```Master``` database on host ```http://84.8.140.1```: After the ```Master``` database gets updated, the ```Slave``` database will be synced for the data that has been inserted/updated/deleted from the main shard, we create database replication due to:
   * Actual workload of the data is a lot larger, having 1 database to endure both read and write operations can lead to delay in reading performance
   * Define roles for analytics and tech team => Only tech-involved members could have master access on the main shard for data structure manipulation, for Analytics members, it should be read-only access
  
# Data model breakdown
```user_management_country``` table: Table used to represent all countries that we can collect the data on the Ecommerce platforms
```user_management_marketplace``` table: Table used to represent all marketplaces that we currently representing ourselves on
```ecommerce_sku``` table: Table used to represent all SKUs-related informations (All SKUs information of 1 shard will be stored in this table only)
```ecommerce_seller``` table: Table used to represent all sellers-related information (All sellers information of 1 shard will be stored in this table only)
```ecommerce_export_sku_sales``` table: Table used to represent sales-performance information (All sales information of 1 shard will be stored in this table only)
