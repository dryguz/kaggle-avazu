import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType, IntegerType


def define_schema(timestamp_fields, date_fields, integer_fields, string_fields):

    string_variables = [StructField(name=string_filed,
                                    dataType=StringType(),
                                    nullable=True)
                        for string_filed in string_fields]

    timestamp_variables = [StructField(name=timestamp_field,
                                       dataType=TimestampType(),
                                       nullable=False)
                           for timestamp_field in timestamp_fields]

    date_variables = [StructField(name=date_field,
                                  dataType=DateType(),
                                  nullable=False)
                      for date_field in date_fields]

    long_variables = [StructField(name=integer_field,
                                  dataType=LongType(),
                                  nullable=True)
                      for integer_field in integer_fields]

    input_data_schema = StructType(long_variables + date_variables + timestamp_variables + string_variables)

    return input_data_schema


if __name__ == '__main___':
    # batch
    example_parquet_file = 'data/split_by_date/date=2014-10-21/hour=00/ckyoimmbwi.parquet'
    spark_df = spark.read.format('parquet').load(example_parquet_file)
    df = pd.read_parquet(example_parquet_file)

    # including schema from folder structure
    input_folder = 'data/input_data/'
    static_data = spark.read.parquet(input_folder)

    static_data = spark.read.option("mergeSchema", "true").parquet(input_folder)
    static_data = spark.read.schema(input_data_schema).format('parquet').load(input_folder)

    # create a stream reader
    from pyspark.sql.functions import window
    stream_data = spark.readStream.schema(input_data_schema).parquet(input_folder)
    id_counts = stream_data.groupBy(window(stream_data.hour, "1 hour", '5 minutes'), stream_data.id).count()

    id_counts.writeStream.format("console").outputMode("append").start().awaitTermination()
