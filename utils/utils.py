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
    example_parquet_file = 'data/split_by_date/date=2014-10-21/time=00/ehuppgnxdj.parquet'
    spark_df = spark.read.format('parquet').load(example_parquet_file)
    df = pd.read_parquet(example_parquet_file)

    print(df.dtypes)
    spark_df.printSchema()

