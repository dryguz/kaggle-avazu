from pyspark.sql import SparkSession
from pyspark.sql import functions as fun

from utils.constans import streaming_data_path
from utils.utils import define_schema


if __name__ == '__main___':

    spark = SparkSession.builder.appName('static').getOrCreate()
    schema = define_schema()
    static_data = spark.read.option("mergeSchema", "true").schema(schema).parquet(streaming_data_path)

    # check no nulls
    for col in static_data.columns:
        print(col, static_data.filter(static_data[col].isNull()).count())
    # no nulls

    # check how many distinct values
    for col in static_data.columns:
        print(col, static_data.select(col).distinct().count())

    # transformation of data for modeling
    ['site_category', 'app_category']

    # get occurrence
    def occurrence_distribution(df, col):
        total = df.select(col).count()
        extended = df.groupby(col).agg(
            fun.round((fun.count(col) / total), scale=5).alias('occurrence')).show()
        return extended

    # pivot occurrence and join
    code_here = []

    static_data.withColumn('site_occurrence', occurrence_distribution(static_data, 'site_category'))

