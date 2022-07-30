import os
import pandas as pd
import configparser
from datetime import datetime
from pyspark.sql import SparkSession

import utils
import etl_functions
import pyspark.sql.functions as F
from quality_checks import *




config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
    Create spark session and add package read sas7bdat data.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark



def process_datasets_to_dim_fact_tables(spark, \
                                         output_data, \
                                         immigration_file_path, \
                                         temperature_file_path, \
                                         i94res_mapping_file_path, \
                                         demographics_file_path):
    """
    Transform immigration dataset to create dim_visatypes, dim_calendar, 
    dim_visatypes and dim_countries tables.
    """
    print("\n>>>>> load immigration_dataset")
    immigration_dataset = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file_path)
    
    print("\n>>>>> limit rows of immigration_dataset for better speed performance")
    immigration_dataset = immigration_dataset.limit(500)
    
    print("\n>>>>> clean immigration_dataset")
    immigration_dataset = utils.clean_immigration_dataset_spark(immigration_dataset)
    
    print("\n>>>>> create dim_visatypes")
    dim_visatypes = etl_functions.dim_visatypes_spark(immigration_dataset, output_data)
    
    print("\n>>>>> create dim_calendar")
    dim_calendar = etl_functions.dim_calendar_spark(immigration_dataset, output_data)
    
    print("\n>>>>> create temperature_dataset")
    temperature_dataset = dim_temperatures(spark, temperature_file_path)
    
    print("\n>>>>> load the i94res_dataset")
    i94res_mapping_dataset = spark.read.csv(i94res_mapping_file_path, header=True, inferSchema=True)
    
    print("\n>>>>> create dim_countries by mapping temperature_dataset with immigration_dataset using i94res_mapping_dataset ")
    dim_countries = etl_functions.dim_countries_spark(spark, \
                                                      immigration_dataset, \
                                                      temperature_dataset, \
                                                      output_data, \
                                                      i94res_mapping_dataset)
    
    print("\n>>>>> load demographics data")
    demographics_dataset = spark.read.csv(demographics_file_path, inferSchema=True, header=True, sep=';')
    print("\n>>>>> clean demographics data")
    demographics_dataset = utils.clean_demographics_dataset_spark(demographics_dataset)
    print("\n>>>>> create demographic dimensional table")
    dim_demographics = etl_functions.dim_demographics_spark(demographics_dataset, output_data)
    
    
    print("\n>>>>> create immigration fact table")
    fact_immigration = etl_functions.fact_immigration_spark(spark, \
                                                            immigration_dataset, \
                                                            output_data, \
                                                            dim_visatypes)
    
    return [fact_immigration, dim_visatypes, dim_calendar, dim_countries, dim_demographics]



def dim_temperatures(spark, temperature_file_path):
    """
    Transform temperatures dataset to limit rows and clean it.
    """
    print("\n>>>>> load temperature_dataset")
    temperature_dataset = spark.read.csv(temperature_file_path, header=True, inferSchema=True)
    
    print("\n>>>>> limit data rows for better processing speed")
    temperature_dataset = temperature_dataset.limit(800)
    
    print("\n>>>>> clean the temperature_dataset")
    temperature_dataset = utils.clean_temperatures_dataset_spark(temperature_dataset)

    return temperature_dataset



# name_dfs_mapping = {
#     'immigration_fact': fact_immigration,
#     'visa_type_dim': dim_visatypes,
#     'calendar_dim': dim_calendar,
#     'usa_demographics_dim': dim_demographics,
#     'country_dim': dim_countries
# }



# views_columns_mapping = {
#     'immigration_fact' : ['cicid', 'i94yr', 'i94mon',
#                          'i94cit', 'country_residence_code', 'i94port',
#                          'arrdate', 'i94mode', 'state_code',
#                          'depdate', 'i94bir', 'i94visa',
#                          'count', 'dtadfile', 'visapost',
#                          'entdepa', 'entdepd', 'matflag',
#                          'biryear', 'dtaddto', 'gender',
#                          'airline', 'admnum', 'fltno',
#                          'visa_type_key'],
#     'visa_type_dim':['visa_type', 'visa_type_key'],
#     'calendar_dim': ['id', 'arrdate', 'arrival_day', 'arrival_month',
#                      'arrival_weekday', 'arrival_week', 'arrival_year'],
#     'usa_demographics_dim':['state_code', 'total_population', 'male_population', 
#                             'female_population', 'median_age', 'number_of_veterans', 
#                             'average_household_size', 'foreign_born',
#                             'count', 'race', 'id', 'city', 'state'],
#     'country_dim':['average_temperature', 'country_code', 'country_name']
# }



def main():
    spark = create_spark_session()
    # input_data = "s3://capstone_project/"     # for running on Amazon S3
    # output_data = "s3://capstone_project/"
    input_data = "./inputs/"                    # for running on local machine 
    output_data = "./outputs/"

    immigration_file_path = input_data + '../../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    temperature_file_path = input_data + '../../../data2/GlobalLandTemperaturesByCity.csv'
    demographics_file_path = input_data + 'us-cities-demographics.csv'
    i94res_mapping_file_path = input_data + "i94res.csv"
    
    (fact_immigration, \
     dim_visatypes, \
     dim_calendar, \
     dim_countries, \
     dim_demographics) = process_datasets_to_dim_fact_tables(spark, \
                                                             output_data, \
                                                             immigration_file_path, \
                                                             temperature_file_path, \
                                                             i94res_mapping_file_path, \
                                                             demographics_file_path)
    
#     # data quality checks
#     tables = {'fact_immigration': fact_immigration,
#              'dim_visatypes': dim_visatypes,
#              'dim_calendar': dim_calendar,
#              'dim_demographics': dim_demographics,
#              'dim_countries': dim_countries}
    
#     for table_name, dim_fact_tables in tables.items():
#         etl_functions.quality_checks(dim_fact_tables, table_name)
#         dim_fact_tables.show(5, truncate=False)

    
    name_dfs_mapping, views_columns_mapping = mapping_objects(
        fact_immigration, 
        dim_visatypes, 
        dim_calendar, 
        dim_demographics, 
        dim_countries
    )
    quality_checks_zero_record(name_dfs_mapping)
    create_view_from_df(name_dfs_mapping)
    quality_checks_column_null(spark, views_columns_mapping)
    quality_checks_schema_completeness(name_dfs_mapping, views_columns_mapping)


if __name__ == "__main__":
    main()
