import os
import configparser
import datetime as dt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from etl_functions import *
from utils import *
# from etl import fact_immigration, dim_visatypes, dim_calendar, dim_demographics, dim_countries


# spark = SparkSession.builder \
#     .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
#     .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
#     .enableHiveSupport() \
#     .getOrCreate()




# fact_immigration = spark.read.csv("./csv_dim_fact/fact_immigration.csv", header=True, inferSchema=True)
# dim_visatypes = spark.read.csv("./csv_dim_fact/dim_visatypes.csv", header=True, inferSchema=True)
# dim_calendar = spark.read.csv("./csv_dim_fact/dim_calendar.csv", header=True, inferSchema=True)
# dim_demographics = spark.read.csv("./csv_dim_fact/dim_demographics.csv", header=True, inferSchema=True)
# dim_countries = spark.read.csv("./csv_dim_fact/dim_countries.csv", header=True, inferSchema=True)




def quality_checks_zero_record(name_dfs_mapping):
    print("\n---> Performing data quality check for record numbers")
    
    for df_name, df in name_dfs_mapping.items():
        total_count = df.count()

        if total_count == 0:
            print(f"Data quality check failed for {df_name} with zero records!")
        else:
            print(f"Data quality check passed for {df_name} with {total_count:,} records.")



def create_view_from_df(name_dfs_mapping):
    print("\n---> Creating view for data quality checks")
    for df_name, df in name_dfs_mapping.items():
        df.createOrReplaceTempView("{}".format(df_name))

        
        
def quality_checks_column_null(spark, views_columns_mapping):
    # spark or spark context (ctx)
    print("\n---> Performing data quality check for null columns")
    
    for view in views_columns_mapping:
        # iterate through keys of dictionary "views_columns_mapping"
        for column in views_columns_mapping[view]:

            querry_info = spark.sql(f"""SELECT COUNT(*) as record_nums FROM {view}""")
            if querry_info.head()[0] == 0:
                print(f"Found NULL values in column {column} of view {view}")
                
        print(f"{view}: Passed")

        
        
def quality_checks_schema_completeness(name_dfs_mapping, views_columns_mapping):
    
    print("\n---> Performing data quality check for schema completeness")
    
    for views_1, dfs in name_dfs_mapping.items():
        for views_2, columns_list in views_columns_mapping.items():
            if views_1 == views_2:
                # print('dfs.columns', dfs.columns)
                # print('columns_list', columns_list)
                missing_schema_condition = list(set(columns_list)-set(dfs.columns)) + \
                                           list(set(dfs.columns)-set(columns_list))
                if dfs.columns == columns_list  \
                or missing_schema_condition == []:
                    # if dfs.columns in columns_list:
                    # if not condition_check:
                    print(f"{views_1}: Pass")
                else:
                    print(list(set(columns_list)-set(dfs.columns))+list(set(dfs.columns)-set(columns_list)))
                    print(f"Missing columns in DF {views_1}: {list(set(columns_list) - set(dfs.columns))}")
            else:
                pass        
        


def mapping_objects(fact_immigration, dim_visatypes, dim_calendar, dim_demographics, dim_countries):

    name_dfs_mapping = {
        'immigration_fact': fact_immigration,
        'visa_type_dim': dim_visatypes,
        'calendar_dim': dim_calendar,
        'usa_demographics_dim': dim_demographics,
        'country_dim': dim_countries
    }



    views_columns_mapping = {
        'immigration_fact' : ['cicid', 'i94yr', 'i94mon',
                              'i94cit', 'country_residence_code', 'i94port',
                              'arrdate', 'i94mode', 'state_code',
                              'depdate', 'i94bir', 'i94visa',
                              'count', 'dtadfile', 'visapost',
                              'entdepa', 'entdepd', 'matflag',
                              'biryear', 'dtaddto', 'gender',
                              'airline', 'admnum', 'fltno',
                              'visa_type_key'],
        'visa_type_dim':['visa_type', 'visa_type_key'],
        'calendar_dim': ['id', 'arrdate', 'arrival_day', 'arrival_month',
                         'arrival_weekday', 'arrival_week', 'arrival_year'],
        'usa_demographics_dim':['state_code', 'total_population', 'male_population', 
                                'female_population', 'median_age', 'number_of_veterans', 
                                'average_household_size', 'foreign_born',
                                'count', 'race', 'id', 'city', 'state'],
        'country_dim':['average_temperature', 'country_code', 'country_name']
    }
    
    return name_dfs_mapping, views_columns_mapping

    

    
    

# if __name__ == "__main__":
#     quality_checks_zero_record(name_dfs_mapping)
#     create_view_from_df(name_dfs_mapping)
#     quality_checks_column_null(spark, views_columns_mapping)
#     quality_checks_schema_completeness(name_dfs_mapping, views_columns_mapping)


















































