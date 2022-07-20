import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import datetime as dt
import requests

import pyspark.sql.functions as F
import pyspark.sql.types as T




requests.packages.urllib3.disable_warnings()

"""
    # def visualize_missing_values(df):

    #     # lets explore missing values per column
    #     nulls_df = pd.DataFrame(data= df.isnull().sum(), columns='values')
    #     nulls_df = nulls_df.reset_index()
    #     nulls_df.columns = ['cols', 'values']

    #     # calculate % missing values
    #     nulls_df['% missing values'] = 100*nulls_df['values']/df.shape[0]

    #     plt.rcdefaults()
    #     plt.figure(figsize=(10,5))
    #     ax = sns.barplot(x="cols", y="% missing values", data=nulls_df)
    #     ax.set_ylim(0, 100)
    #     ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    #     plt.show()
"""


def plot_missing_values_spark(data_frame):
    """
    Plot chart of missing values (spark dataframe)
    """
    # create a data_frame with missing values count columns
    nan_count_df = data_frame.select([count(F.when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_frame.columns]) \
                     .toPandas()
    # convert pandas dataframe from wide format to long format
    nan_count_df = pd.melt(nan_count_df, var_name='cols', value_name='values')
    # create a 'percent_of_missing' column
    nan_count_df['percent_of_missing'] = 100 * nan_count_df['values'] / data_frame.count() # count total records in missing value data_frame
    
    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="percent_of_missing", data=nan_count_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()
    
    
    
"""
    # def clean_immigration(df):

    #     # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    #     drop_columns = ['occup', 'entdepu','insnum']
    #     df = df.drop(columns=drop_columns)
    #     # drop rows where all elements are missing
    #     df = df.dropna(how='all')

    #     return df
"""



def clean_immigration_dataset_spark(immigration_dataset):
    """
    Clean immigration_dataset (spark dataframe)
    """
    total_rows = immigration_dataset.count()
    print(f'Total rows in immigration_dataset: {total_rows:,}')
    
    drop_columns = ['occup', 'entdepu','insnum'] # columns with 90% null -> drop
    immigration_dataset = immigration_dataset.drop(*drop_columns)
    
    immigration_dataset = immigration_dataset.dropna(how='all')
    immigration_dataset = immigration_dataset.drop_duplicates(subset=immigration_dataset.columns)
    new_total_rows = immigration_dataset.count()
    print(f'Total rows after cleaning: {new_total_rows:,}')
    
    return immigration_dataset



"""
    # def clean_temperature_data(df):

    #     # drop rows with missing average temperature
    #     df = df.dropna(subset=['AverageTemperature'])

    #     # drop duplicate rows
    #     df = df.drop_duplicates(subset=['dt', 'City', 'Country'])

    #     return df
"""



def clean_temperatures_dataset_spark(temperature_dataset):
    """
    Clean global temperatures_dataset (spark dataframe)
    """
    total_rows = temperature_dataset.count()
    print(f'Total rows in temperature_dataset: {total_rows:,}')
    
    # drop rows with missing average temperature
    temperature_dataset = temperature_dataset.dropna(subset=['AverageTemperature'])
    
    new_total_rows = temperature_dataset.count()
    print('Total rows after dropna: {:,}'.format(total_rows - new_total_rows))
    temperature_dataset = temperature_dataset.drop_duplicates(subset=['dt', 'City', 'Country'])
    print('Rows dropped after accounting for duplicates: {:,}'.format(new_total_rows - temperature_dataset.count()))
    
    return temperature_dataset



def aggregate_temperature_data(temperature_dataset):
    """
    Aggregate cleaned temperature_dataset to get average_temperature column
    """
    agg_temperature_dataset = temperature_dataset.select(F.col('Country'), F.col('AverageTemperature')) \
                                                 .groupby(F.col('Country')) \
                                                 .avg()

    agg_temperature_dataset = agg_temperature_dataset.withColumnRenamed('avg(AverageTemperature)', \
                                                                        'average_temperature')
    # note: new column renamed => not use F.col('...') 
    
    return agg_temperature_dataset


"""
    # def clean_demographics_data(df):
    #     # drop rows with missing values
    #     subset_cols = [
    #         'Male Population',
    #         'Female Population',
    #         'Number of Veterans',
    #         'Foreign-born',
    #         'Average Household Size'
    #     ]
    #     df = df.dropna(subset=subset_cols)

    #     # drop duplicate columns
    #     df = df.drop_duplicates(subset=['City', 'State', 'State Code', 'Race'])

    #     return df
"""


def clean_demographics_dataset_spark(demographics_dataset):
    """
    Clean demographics_dataset (spark dataframe)
    """
    # columns to check for missing values
    processing_dropna_cols = ['Male Population',
                              'Female Population',
                              'Number of Veterans',
                              'Foreign-born',
                              'Average Household Size'
                             ]
    dataset_dropna = demographics_dataset.dropna(subset=processing_dropna_cols)
    
    num_rows_dropped = demographics_dataset.count() - dataset_dropna.count()
    print("Missing value rows dropped: {}".format(num_rows_dropped))
    
    # columns to dropping duplicate values
    processing_drop_duplicates_cols = ['City', 'State', 'State Code', 'Race']
    dataset_drop_duplicates = dataset_dropna.dropDuplicates(subset=processing_drop_duplicates_cols)
    
    num_duplicate_rows_dropped = dataset_dropna.count() - dataset_drop_duplicates.count()
    print("Duplicate value rows dropped: {}".format(num_duplicate_rows_dropped))
    
    return dataset_drop_duplicates


"""
    # def print_formatted_float(number):
    #     print('{:,}'.format(number))
"""