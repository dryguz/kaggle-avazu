import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark = SparkSession.builder.appName('avazu').getOrCreate()


# batch
example_parquet_file = 'data/split_by_date/date=2014-10-21/hour=00/ckyoimmbwi.parquet'
spark_df = spark.read.format('parquet').load(example_parquet_file)
df = pd.read_parquet(example_parquet_file)


# fields
string_fields = ['id', 'hour', 'C1', 'banner_pos', 'site_id', 'site_domain', 'site_category', 'app_id',
                 'app_domain', 'app_category', 'device_id', 'device_ip', 'device_model', 'device_type',
                 'device_conn_type', 'C14', 'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21']
string_types = [StructField(name=string_filed, dataType=StringType(), nullable=True) for string_filed in string_fields]
long_fields = ['click']
long_types = [StructField(name=long_field, dataType=LongType(), nullable=True) for long_field in long_fields]


# schema defined upfront
input_data_schema = StructType(long_types + string_types)
read_parquet_with_schema = spark.read.schema(input_data_schema).parquet(example_parquet_file)


# including schema from folder structure
input_folder = 'data/input_data'
input_data = spark.read.format('parquet').load(input_folder)


# create a stream reader
stream_data = spark.readStream.schema(input_data_schema).parquet(input_folder)
stream_data.writeStream.start()
