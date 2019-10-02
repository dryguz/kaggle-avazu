import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType, IntegerType

spark = SparkSession.builder.appName('avazu').getOrCreate()


# batch
example_parquet_file = 'data/split_by_date/date=2014-10-21/hour=00/ckyoimmbwi.parquet'
spark_df = spark.read.format('parquet').load(example_parquet_file)
df = pd.read_parquet(example_parquet_file)

| -- id: string(nullable=true)
| -- click: long(nullable=true)
| -- hour: timestamp(nullable=true)
| -- C1: long(nullable=true)
| -- banner_pos: long(nullable=true)
| -- site_id: string(nullable=true)
| -- site_domain: string(nullable=true)
| -- site_category: string(nullable=true)
| -- app_id: string(nullable=true)
| -- app_domain: string(nullable=true)
| -- app_category: string(nullable=true)
| -- device_id: string(nullable=true)
| -- device_ip: string(nullable=true)
| -- device_model: string(nullable=true)
| -- device_type: long(nullable=true)
| -- device_conn_type: long(nullable=true)
| -- C14: long(nullable=true)
| -- C15: long(nullable=true)
| -- C16: long(nullable=true)
| -- C17: long(nullable=true)
| -- C18: long(nullable=true)
| -- C19: long(nullable=true)
| -- C20: long(nullable=true)
| -- C21: long(nullable=true)
| -- __index_level_0__: long(nullable=true)
| -- date: date(nullable=true)
| -- time: integer(nullable=true)

# fields
string_fields = ['id', 'C1', 'banner_pos', 'site_id', 'site_domain', 'site_category', 'app_id',
                 'app_domain', 'app_category', 'device_id', 'device_ip', 'device_model', 'device_type',
                 'device_conn_type', 'C14', 'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21']
string_types = [StructField(name=string_filed,
                            dataType=StringType(),
                            nullable=True) for string_filed in string_fields]

timestamp_types = [StructField(name='hour', dataType=TimestampType(), nullable=False)]
date_types = [StructField(name='date', dataType=DateType(), nullable=False)]
integer_types = [StructField(name='click', dataType=LongType(), nullable=True),
                 StructField(name='time', dataType=IntegerType(), nullable=False)]


# schema defined upfront
input_data_schema = StructType(integer_types + date_types + timestamp_types + string_types)


# including schema from folder structure
input_folder = 'data/input_data/'
static_data = spark.read.parquet(input_folder)
static_data = spark.read.option("mergeSchema", "true").parquet(input_folder)



static_data = spark.read.schema(input_data_schema).format('parquet').load(input_folder)
static_data = spark.read.format('parquet').load(input_folder)

# create a stream reader
from pyspark.sql.functions import window
stream_data = spark.readStream.schema(input_data_schema).parquet(input_folder)
id_counts = stream_data.groupBy(window(stream_data.hour, "1 hour", '5 minutes'), stream_data.id).count()

id_counts.writeStream.format("console").outputMode("append").start().awaitTermination()
