import datetime
import logging
import os
import shutil
import sys

from pyspark.sql.functions import *
from pyspark.sql.functions import max
from pyspark.sql.functions import concat_ws, lit, col, expr
from pyspark.sql.types import StructField, IntegerType, StringType, DateType, StructType, FloatType

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()
response = s3_client.list_buckets()
#print(response)

logger.info("List of Buckets: %s", response['Buckets'])

# ##### Read files on s3 without download
# bucket_name = "data-engineering-project-01"
# path = "temp/msft.csv"
# response = s3_client.get_object(Bucket=bucket_name, Key=path)
# print(response)
# file_content = response['Body'].read().decode('utf-8')

# print(file_content)
# logger.info(file_content)
####

# Check if local directory has already a file
# If file is there then check if same file is in staging table
# with status as A if so then don't delete try rerun
# Else give an error and don't process next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
logger.info(csv_files)

connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []

if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    logger.info("Total csv files:%s", total_csv_files)

    statement = f"""select distinct file_name from {config.database_name}.{config.product_staging_table}
                             where file_name in ({(', '.join(['%s'] * len(total_csv_files)))}) and status='A' """
    logger.info(f"Dynamically select statement created {statement}")
    cursor.execute(statement, total_csv_files)
    data = cursor.fetchall()
    logger.info(f"Return records from table {data}")

    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No record Match")
else:
    logger.info("Last run was successful")

# Read files on s3

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    #bucket name should come from table
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info("Absolute path for csv files on s3 buckets %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")
except Exception as e:
    #print("K")
    logger.error("Exited with error : %s", e)
    raise e

prefix = f's3://{config.bucket_name}/'
local_directory = config.local_directory
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("Files available on s3 under bucket %s name and folder name %s", prefix, file_paths)

try:
    downloader = S3FileDownloader(s3_client, config.bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File downloader error %s", e)
    sys.exit()

#Get a list of all files in the local directory

all_files = os.listdir(local_directory)
logger.info(f"List al the files present on my local directory after download: {all_files}")

if all_files:
    csv_files = []
    error_files = []

    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))
    if not csv_files:
        logger.error("No data available to process the request")
        raise Exception("No data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("**********************Listing the Files*************************")
logger.info("List of csv files needs to be processed: %s", csv_files)

logger.info("**********************Creating Spark Session*************************")
spark = spark_session()
logger.info("**********************Spark Session Created*************************")

#Check required columns in schema of csv files
#If not required column keep it in list of error files
#Else union data in one dataframe


logger.info("******************Checking schema for data loaded in S3*********************")

corrected_files = []

for data in csv_files:
    data_schema = spark.read.format("csv") \
        .option("header", "true") \
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns in schema are {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info("Missing columns are: %s", missing_columns)

    if missing_columns:
        error_files.append(data)
    else:
        logger.info("No missing columns for: %s", data)
        corrected_files.append(data)

logger.info(f"**************List of corrected file: {corrected_files}****************")
logger.info(f"**************List of error file: {error_files}****************")

logger.info("Processing error files to error directory if any********")

#Move data to error directory on local
error_folder_local_path = config.error_folder_path_local

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved {file_name} from s3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"{file_path} does not exist")

else:
    logger.info("No error files to available at our dataset")

#Additional columns needs to be taken care of
#Determine extra columns

#Before running process
#Stage table needs to be updated with status as Active(A) or Inactive(I)

logger.info("********Updating the product_staging_table that we hava started the process")
insert_statement = []
db_name = config.database_name
current_date = datetime.datetime.now()
formated_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if corrected_files:
    for file in corrected_files:
        filename = os.path.basename(file)
        statement = f"INSERT INTO {db_name}.{config.product_staging_table}" \
                    f"(file_name, file_location, created_date, status)" \
                    f" VALUES('{filename}','{filename}','{formated_date}','A')"
        insert_statement.append(statement)
    logger.info("Insert statement created for staging table %s", insert_statement)

    logger.info("**********Connecting to mysql server**********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******** Successfully connected to MYSQL server")
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
else:
    logger.error("********There are no files to process********")
    raise Exception("*********No data available with corrected files")
logger.info("Staging table updated successfully")

logger.info("********Fixing extra Columns comming from source")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema, 5)

#Create new column with concatenated values of extra column
for data in corrected_files:
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info("Extra columns in corrected files are: %s", extra_columns)
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("This is the final df which wii be process further")
final_df_to_process.show()

#Enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segrigation
#Read the data from parquet and generate a csv file
#in which there will be a sales_person_name, sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive

#Connecting with database reader
database_connect = DatabaseReader(config.url, config.properties)

# customer_table_name = "customer"
# product_staging_table = "product_staging_table"
# product_table = "product"
# sales_team_table = "sales_team"
# store_table = "store"

logger.info("Loading customer data into customer_table_df")
customer_table_df = database_connect.create_dataframe(spark, config.customer_table_name)

logger.info("Loading product_table data into product_table_df")
product_table_df = database_connect.create_dataframe(spark, config.product_table)

logger.info("Loading sales_team_table data into sales_team_table_df")
sales_team_table_df = database_connect.create_dataframe(spark, config.sales_team_table)

logger.info("Loading store_table data into store_table_df")
store_table_df = database_connect.create_dataframe(spark, config.store_table)

logger.info("Loading product_staging_table data into product_staging_table_df")
product_staging_table_df = database_connect.create_dataframe(spark, config.product_staging_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                       customer_table_df,
                                                       store_table_df,
                                                       sales_team_table_df)

logger.info("*******************Final Enriched Data*******************")
s3_customer_store_sales_df_join.show()

#Writing customer data in customer data mart in parquet format
#First file will be writter to local host
#Move RAW data to s3 for reporting
#Writin reporting data in mysql also

logger.info("****************Writing customer data in customer data mart*********************")
final_customer_mart_data_df = s3_customer_store_sales_df_join \
    .select("ct.customer_id", "ct.first_name", "ct.last_name",
            "ct.address", "ct.pincode", "phone_number",
            "sales_date", "total_cost")
logger.info("****************Final data for customer data mart*********************")
final_customer_mart_data_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_mart_data_df, config.customer_data_mart_local_file)

logger.info("*********Data movement from local to s3 for customer data mart***********")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales team data mart
logger.info("****************Writing sales data in sales tesm data mart*********************")
final_sales_team_mart_data_df = s3_customer_store_sales_df_join. \
    select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name", \
           "store_manager_name", "manager_id", "is_manager", "sales_person_address",
           "sales_person_pincode", "sales_date", "total_cost", \
           expr("SUBSTRING(sales_date, 1, 7) as sales_month"))

logger.info("****************Final data for sales team data mart*********************")
final_sales_team_mart_data_df.show()
final_sales_team_mart_data_df.printSchema()
# final_sales_team_mart_data_df.write.csv(r"C:\Users\Rupali\Downloads\final_sales_team_mart_data_df")


# test = final_sales_team_mart_data_df\
#     .filter((col("store_id")==121) & (col("sales_person_id")==2) & (col("sales_month")=='2023-03')).withColumn("total_cost", col("total_cost").cast("double")).agg(sum("total_cost"))
#
# test.show()
# test.write.csv(r"C:\Users\Rupali\Downloads\final_sales_team_mart_data_df")
# test = final_sales_team_mart_data_df.agg(sum("total_cost"))
#
#
#
# print("This is test")
# print(test.collect())


#"store_id","sales_person_id", "sales_month"
parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_sales_team_mart_data_df, config.sales_team_data_mart_local_file)
#
logger.info("*********Data movement from local to s3 for sales team data mart***********")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")
#
#Writting data in partition
final_sales_team_mart_data_df.write.format("parquet") \
    .option("header", "true") \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .option("path", config.sales_team_data_mart_partitioned_local_file) \
    .save()

#Move data on s3 for partition folder
#
s3_prefix = "sales_team_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
print(current_epoch)
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    print(f"{root} {dirs} {files}")
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        print(local_file_path)
        print(config.sales_team_data_mart_partitioned_local_file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        print(relative_file_path)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        print(s3_key)
        message = s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

#Calculation for customer mart
#Find out customer total purchase every month
#Write data in Mysql table

logger.info("Calculating customers every month purchase amount")
customer_mart_calculation_table_write(final_customer_mart_data_df)

logger.info("Calculations for sales team mart")
#Find out total sales done by each sales person every month
#give top performer 1% incentive
#Write data in mysql table


logger.info("Calculating sales team every month sales by sales person")
sales_calculation_table_write(final_sales_team_mart_data_df)

#Move the file to s3 on process folder and deete from loacal
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("*********Deleting local directory********")
delete_local_file(config.local_directory)
logger.info("*********Deleted local directory********")

logger.info("*********Deleting customer data from local********")
delete_local_file(config.customer_data_mart_local_file)
logger.info("*********Deleted customer data from local********")

logger.info("*********Deleting sales data mart from local********")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("*********Deleted sales data mart from local********")

logger.info("*********Deleting sales partitioned data mart from local********")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("*********Deleted sales partitioned data mart from local********")

#update status of staging table
update_statement = []
if corrected_files:
    for file in corrected_files:
        filename = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table}" \
                     f" SET status = 'I',updated_date = '{formated_date}' " \
                     f"WHERE file_name = '{file_name}'"
        update_statement.append(statements)

    logger.info(f"********Updated statement for staging table ------ {update_statement}********")
    logger.info("**********Connecting to mysql database*********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("********Connected to MYSql successfully************")

    for statement in update_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("There is some error in between")
    sys.exit()

input("Press Enter to terminate")
