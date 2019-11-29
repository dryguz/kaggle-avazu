import pandas as pd
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType
from utils.constans import timestamp_fields, date_fields, long_fields, string_fields


def define_schema():

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
                      for integer_field in long_fields]

    input_data_schema = StructType(long_variables + date_variables + timestamp_variables + string_variables)
    return input_data_schema

