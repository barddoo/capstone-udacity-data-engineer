import os

import configparser
from pyspark.sql import SparkSession

import etl_functions

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0") \
        .getOrCreate()

    return spark


def process_economy_data(spark, output_data):
    """
    ETL Brazilian economy data from world bank
    :param spark: (SparkSession) spark session instance
    :param output_data: (string) output file path
    :return: spark dataframe representing economy table
    """

    economy_df = etl_functions.create_economy_pandas()
    economy_df = spark.createDataFrame(economy_df)
    return etl_functions.create_economy_table(economy_df, output_data)


def process_trading_data(spark, trading_files, output_data):
    """
    ETL trading data.
    :param spark: (SparkSession) spark session instance
    :param trading_files: (string) input file path
    :param output_data: (string) output file path
    :return: spark dataframe of trading data
    """

    trading_df = spark.read.text(paths=trading_files)

    trading_df = etl_functions.raw_trading_to_pandas(trading_df)
    trading_df = etl_functions.trading_pandas_to_spark(spark, trading_df)

    return etl_functions.create_trading_table(trading_df, output_data)


def quality_check(df, name):
    """
    Count checks on table to ensure completeness of data.
    :param df: spark dataframe to check counts on
    :param name: name of the dataframe
    """

    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {name} with zero records!")
    else:
        print(f"Data quality check passed for {name} with {total_count} records.")
    return 0


def quality_check_column(df, column):
    """
    Count checks on table to ensure completeness of data.
    :param df: spark dataframe to check counts on
    :param column: name of the column
    """

    total_count = df.selet(column).count()

    if total_count == 0:
        print(f"Data quality check failed for table {column} with zero records!")
    else:
        print(f"Data quality check passed for table {column} with {total_count} records.")
    return 0


def main():
    spark = create_spark_session()
    input_data = "sample_data"
    output_data = "sample_data/output"

    trading_files = os.path.join(input_data, "COTAHIST_A*.txt")

    trading_df = process_trading_data(spark, trading_files, output_data)
    economy_df = process_economy_data(spark, output_data)

    quality_check(economy_df, 'economy')
    quality_check(trading_df, 'trading')
    quality_check(trading_df, 'stock_code')
    quality_check(trading_df, 'date')
    quality_check(trading_df, 'volume')
    quality_check(economy_df, 'table')
    quality_check(economy_df, 'year')


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        print(e)
