from pyspark.sql import SparkSession

from constans import timestamp_fields, date_fields, long_fields, string_fields
from constans import streaming_data_path
from utils import define_schema


if __name__ == '__main___':

    spark = SparkSession.builder.appName('static').getOrCreate()
    schema = define_schema(timestamp_fields, date_fields, long_fields, string_fields)
    static_data = spark.read.option("mergeSchema", "true").schema(schema).parquet(streaming_data_path)

    # fill nulls
    static_data.na.fill()

    static_data.columns.isNull().count()

    for col in static_data.columns:
        print(col, static_data.filter(static_data[col].isNull()).count())

    for col in static_data.columns:
        print(col, static_data.select(col).distinct().count())

    ['site_category', 'app_category']

    total_site_category = static_data.select('site_category').count()
    static_data.groupby('site_category').count().show()