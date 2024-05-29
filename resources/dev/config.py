import os

key = "amit_DataEngg_project"
iv = "amit_DEng_encypt"

salt = "amit_DataEngg_AesEncryption"

#AWS Access And Secret key
aws_access_key = b'Your Key'
aws_secret_key = b'Your Secret key'
bucket_name = "data-engineering-project-01"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
sales_partitioned_data_mart_directory = "sales_partitioned_data_mart"


#Database credential
# MySQL database connection properties
database_name = "data_engg_project_01"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

# File Download location
local_directory = "C:\\Users\\Rupali\\PycharmProjects\\youtube_de_project1\\local\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\Rupali\\PycharmProjects\\youtube_de_project1\\local\\customer_data_mart"
sales_team_data_mart_local_file = "C:\\Users\\Rupali\\PycharmProjects\\youtube_de_project1\\local\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\Rupali\\PycharmProjects\\youtube_de_project1\\local\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\Rupali\\PycharmProjects\\youtube_de_project1\\local\\error_files\\"
