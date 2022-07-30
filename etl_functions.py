import os
import configparser
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import utils
import pyspark.sql.functions as F
import pyspark.sql.types as T





global get_datetime
get_datetime = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)


# def fact_immigration_spark(spark, \
#                            immigration_dataset, \
#                            output_data, \
#                            dim_visatypes):
#     """
#     Creates fact_immigration table from immigration_dataset.
#     """
    
#     # create a view for visa type dimension
#     dim_visatypes.createOrReplaceTempView("visa_view")
#     # rename columns to align with data model
#     immigration_dataset = immigration_dataset.withColumnRenamed('ccid', 'record_id') \
#                                              .withColumnRenamed('i94res', 'country_residence_code') \
#                                              .withColumnRenamed('i94addr', 'state_code')
#     # create an immigration view
#     immigration_dataset.createOrReplaceTempView("immigration_view")
#     # create visa_type key
#     immigration_dataset = spark.sql(
#         """
#         SELECT 
#             immigration_view.*, 
#             visa_view.visa_type_key
#         FROM immigration_view
#         LEFT JOIN visa_view ON visa_view.visatype=immigration_view.visatype
#         """
#     )
#     # convert arrival date into datetime object
#     immigration_dataset = immigration_dataset.withColumn("arrdate", get_datetime(immigration_dataset.arrdate))
#     # drop visatype key
#     immigration_dataset = immigration_dataset.drop(immigration_dataset.visatype)
#     # write dimension to parquet file
#     immigration_dataset.write.parquet(output_data + "fact_immigration", mode="overwrite")

#     return immigration_dataset



def fact_immigration_spark(spark, \
                           immigration_dataset, \
                           output_data, \
                           dim_visatypes):
    """
    Creates fact_immigration table from immigration_dataset.
    """
    # rename columns to align with data model
    fact_immigration = immigration_dataset.withColumnRenamed('ccid', 'record_id') \
                                          .withColumnRenamed('i94res', 'country_residence_code') \
                                          .withColumnRenamed('i94addr', 'state_code') \
                                          .withColumnRenamed('visatype', 'visa_type')
    
    initial_immigration_columns = fact_immigration["*"]
    fact_immigration = fact_immigration.select(initial_immigration_columns) \
                                       .join(dim_visatypes, \
                                             fact_immigration.visa_type == dim_visatypes.visa_type, \
                                             'left')\
                                       .distinct()
    # fact_immigration.columns << OR >> fact_immigration['*']
    fact_immigration = fact_immigration.select(initial_immigration_columns, \
                                               F.col('visa_type_key'))
    # because dim_visatypes was derived from immigration dataset => no need to select specific columns for fact_immigration after join.
    # fact_immigration["*"]  ...  return columns type !!!
    
    # create arrival date with datetime type
    fact_immigration = fact_immigration.withColumn("arrdate", get_datetime(fact_immigration.arrdate))

    fact_immigration = fact_immigration.drop(fact_immigration.visa_type)
    fact_immigration.write.parquet(output_data + "fact_immigration", mode="overwrite")

    return fact_immigration
    
    

def dim_demographics_spark(demographics_dataset, output_data):
    """
    Enrich demographics_dataset, add id column => dim_demographics table
    """
    demographics_dataset = demographics_dataset.withColumnRenamed('Median Age', 'median_age') \
                                                .withColumnRenamed('Male Population', 'male_population') \
                                                .withColumnRenamed('Female Population', 'female_population') \
                                                .withColumnRenamed('Total Population', 'total_population') \
                                                .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
                                                .withColumnRenamed('Foreign-born', 'foreign_born') \
                                                .withColumnRenamed('Average Household Size', 'average_household_size') \
                                                .withColumnRenamed('State Code', 'state_code') \
                                                .withColumnRenamed('City', 'city') \
                                                .withColumnRenamed('Count', 'count') \
                                                .withColumnRenamed('Race', 'race') \
                                                .withColumnRenamed('State', 'state')
    
    # ['City', 'Count', 'Race', 'State']  ## add with column renamed

    dim_demographics = demographics_dataset.withColumn('id', F.monotonically_increasing_id())
    dim_demographics.write.parquet(output_data + "dim_demographics", mode="overwrite")

    return dim_demographics


def dim_visatypes_spark(immigration_dataset, output_data):
    """
    Create dim_visatypes from immigration_dataset
    """
    dim_visatypes = immigration_dataset.select('visatype').distinct()
    dim_visatypes = dim_visatypes.withColumnRenamed('visatype', 'visa_type')
    dim_visatypes = dim_visatypes.withColumn('visa_type_key', F.monotonically_increasing_id())
    dim_visatypes.write.parquet(output_data + "dim_visatypes", mode="overwrite")

    return dim_visatypes



    """
    # def get_visa_type_dimension(spark, output_data):
    #     return spark.read.parquet(output_data + "visatype")
    """

    
    
    """
    # def create_country_dimension_table(spark, \
    #                                    df, \
    #                                    temp_df, \
    #                                    output_data, \
    #                                    mapping_file):
    #     # create temporary view for immigration data
    #     df.createOrReplaceTempView("immigration_view")
    #     # create temporary view for countries codes data
    #     mapping_file.createOrReplaceTempView("country_codes_view")
    #     # get the aggregated temperature data
    #     agg_temp = aggregate_temperature_data(temp_df)
    #     # create temporary view for countries average temps data
    #     agg_temp.createOrReplaceTempView("average_temperature_view")
    #     # create country dimension using SQL
    #     country_df = spark.sql(
    #         '''
    #         SELECT 
    #             i94res as country_code,
    #             Name as country_name
    #         FROM immigration_view
    #         LEFT JOIN country_codes_view
    #         ON immigration_view.i94res=country_codes_view.code
    #         '''
    #     ).distinct()
    #     # create temp country view
    #     country_df.createOrReplaceTempView("country_view")
    #     country_df = spark.sql(
    #         '''
    #         SELECT 
    #             country_code,
    #             country_name,
    #             average_temperature
    #         FROM country_view
    #         LEFT JOIN average_temperature_view
    #         ON country_view.country_name=average_temperature_view.Country
    #         '''
    #     ).distinct()
    #     # write the dimension to a parquet file
    #     country_df.write.parquet(output_data + "dim_countries", mode="overwrite")

    #     return country_df
    """


def dim_countries_spark(spark, \
                        immigration_dataset, \
                        temperature_dataset, \
                        output_data, \
                        i94res_mapping_dataset):
    """
    Create dim_countries by mapping the i94res_mapping_dataset with temperature_dataset and 
    """
    immigration_dataset = immigration_dataset.withColumnRenamed('i94res', 'country_code')
    dim_countries = immigration_dataset.join(i94res_mapping_dataset, \
                                             immigration_dataset.country_code == i94res_mapping_dataset.code, \
                                             'left') \
                                       .distinct()
    dim_countries = dim_countries.withColumnRenamed('Name', 'country_name')
    
    agg_temperature_dataset = utils.aggregate_temperature_data(temperature_dataset)
    dim_countries = dim_countries.join(agg_temperature_dataset, \
                                       dim_countries.country_name == agg_temperature_dataset.Country, \
                                       'left') \
                                 .distinct()
    
    dim_countries = dim_countries.select(F.col('country_code'), \
                                         F.col('country_name'), \
                                         F.col('average_temperature'))
    # dim_countries = dim_countries.select('country_code', 'country_name', 'average_temperature')
    dim_countries.write.parquet(output_data + "dim_countries", mode="overwrite")
    
    return dim_countries
    
    

def dim_calendar_spark(immigration_dataset, output_data):
    """
    Create dim_calendar from immigration_dataset
    """

    dim_calendar = immigration_dataset.select(F.col('arrdate')) \
                                      .withColumn("arrdate", get_datetime(immigration_dataset.arrdate)) \
                                      .distinct()

    dim_calendar = dim_calendar.withColumn('arrival_day', F.dayofmonth('arrdate'))
    dim_calendar = dim_calendar.withColumn('arrival_week', F.weekofyear('arrdate'))
    dim_calendar = dim_calendar.withColumn('arrival_month', F.month('arrdate'))
    dim_calendar = dim_calendar.withColumn('arrival_year', F.year('arrdate'))
    dim_calendar = dim_calendar.withColumn('arrival_weekday', F.dayofweek('arrdate'))

    dim_calendar = dim_calendar.withColumn('id', F.monotonically_increasing_id())

    partition_by_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    dim_calendar.write.parquet(output_data + "dim_calendar", partitionBy=partition_by_columns, mode="overwrite")

    return dim_calendar



def quality_checks(dim_fact_tables, table_name):
    """
    Count checks on fact and dimension tables to ensure completeness of data.
    """
    total_count = dim_fact_tables.count()

    if total_count == 0:
        print(f"\nData quality check: FAILED => {table_name} with zero records!")
    else:
        print(f"\nData quality check: PASSED => {table_name} with {total_count:,} records.")
    
    return 0
