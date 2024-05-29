from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def sales_calculation_table_write(final_sales_team_mart_data_df):
    window = Window.partitionBy("store_id","sales_person_id", "sales_month")
    window2 = Window.partitionBy("store_id","sales_month").orderBy(col("total_sales").desc())
    final_sales_data_mart = final_sales_team_mart_data_df\
                    .withColumn("total_sales",
                                sum("total_cost").over(window))\
                    .select("store_id", "sales_person_id", concat(col("sales_person_first_name"),lit(" "),col("sales_person_last_name"))
                            .alias("full_name"), "sales_month","total_sales").distinct()\
                    .withColumn('ranking', dense_rank().over(window2))\
                    .withColumn("incentive",when(col("ranking")==1,col("total_sales")*0.01).otherwise(lit(0))).distinct().drop("ranking")

    #Write the Data into MySQL customers_data_mart table
    final_sales_data_mart.printSchema()
    final_sales_data_mart.show()
    # db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    # db_writer.write_dataframe(final_sales_data_mart,config.customer_data_mart_table)
    #
