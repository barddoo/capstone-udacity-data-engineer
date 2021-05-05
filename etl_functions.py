import os.path

import bovespa
import wbgapi as wb
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

trading_schema = StructType([
    StructField('date', DateType(), True),
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('day', IntegerType(), True),
    StructField('money_volume', DoubleType(), True),
    StructField('volume', IntegerType(), True),
    StructField('stock_code', StringType(), True),
    StructField('company_name', StringType(), True),
    StructField('price_open', DoubleType(), True),
    StructField('price_close', DoubleType(), True),
    StructField('price_mean', DoubleType(), True),
    StructField('price_high', DoubleType(), True),
    StructField('price_low', DoubleType(), True),
    StructField('variation', DoubleType(), True)
])

economy_schema = {
    'GC.DOD.TOTL.GD.ZS': 'debt',
    'GC.XPN.TOTL.CN': 'total_expense',
    'NY.GDP.MKTP.KD.ZG': 'gdp_growth',
    'NY.GDP.MKTP.CD': 'gdp',
    'NY.GDP.PCAP.CD': 'gdp_per_capita',
    'SP.POP.TOTL': 'population',
    'SP.DYN.LE00.IN': 'life_expectancy',
    'GC.XPN.TOTL.GD.ZS': 'expense_per_gdp',
    'FI.RES.TOTL.CD': 'total_reserves',
    'SE.ADT.LITR.ZS': 'pop_literacy_rate',
    'SE.XPD.TOTL.GD.ZS': 'expenditure_education_per_gdp',
    'BX.KLT.DINV.CD.WD': 'foreign_investment'
}


def create_economy_table(df, output_data):
    """
    Write result data
    :param df: spark dataframe of Brazilian economy
    :param output_data: path to write dataframe to
    :return: spark dataframe representing economy dimension
    """

    df.write.parquet(os.path.join(output_data, "economy_fact"), mode="overwrite", compression='gzip')

    return df


def create_trading_table(df, output_data):
    """Write the trading dataframe
    :param df: spark dataframe of trading data
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe.
    """

    df.write.parquet(os.path.join(output_data, "trading"), mode="overwrite", compression='gzip')

    return df


def create_economy_df():
    """
    Load data from world bank
    :return: pandas dataframe representing economy table
    """
    economy_data = get_economy_data_api()
    economy_data = make_year_column(economy_data)
    return economy_data


def make_year_column(economy_data):
    """
    Make year column
    :param economy_data: pandas economy data
    :return: dataframe with year column
    """
    years = list(economy_data.columns)
    economy_data = economy_data.transpose()
    economy_data['year'] = years
    return economy_data


def get_economy_data_api():
    """
    Get economy data from World Bank API
    :return: economy data
    """

    df_raw = wb.data.DataFrame(economy_schema.keys(), economy='BRA', numericTimeKeys=True)
    return df_raw.rename(economy_schema, axis=0)


def raw_trading_to_spark(raw_df):
    """
    Convert dataframe to pandas, transform position string into bovespa.Record, transform it into a dict then
    explode the columns to the schema.
    :param raw_df: (spark dataframe)
    :return: pandas dataframe
    """
    dict_df = raw_df.select(record_to_dict('value').alias('dict'))
    dict_df = dict_df.select("dict.*", "*").drop('dict')
    return dict_df.dropna()


def trading_columns(trading_df):
    """
    Make dimension columns
    :param trading_df: trading dataframe
    :return dataframe with new columns
    """

    return trading_df.withColumn('variation', (trading_df['price_close'] - trading_df['price_open']) / 100)


@F.udf(returnType=trading_schema)
def record_to_dict(record):
    """
    Transform string into bovespa.Record
    :param record: (string) position string from bovespa.
    :return: parsed Record
    """
    try:
        record = bovespa.Record(record)
    except:
        return None
    return {
        'date': record.date, 'year': record.date.year,
        'month': record.date.month, 'day': record.date.day,
        'money_volume': record.volume, 'volume': record.quantity,
        'stock_code': record.stock_code, 'company_name': record.company_name,
        'price_open': record.price_open, 'price_close': record.price_close,
        'price_mean': record.price_mean, 'price_high': record.price_high,
        'price_low': record.price_low
    }


# Perform quality checks here
def quality_check(df_check, name):
    """
    Count checks on table to ensure completeness of data.
    :param df_check: spark dataframe to check counts on
    :param name: name of the dataframe
    """

    total_count = df_check.count()

    if total_count == 0:
        print(f"Data quality check failed for {name} with zero records!")
    else:
        print(f"Data quality check passed for {name} with {total_count} records.")


def quality_check_column(df_check, column):
    """
    Count checks on table to ensure completeness of data.
    :param df_check: spark dataframe to check counts on
    :param column: name of the column
    """

    total_count = df_check.where(df_check[column].isNotNull()).count()

    if total_count == 0:
        print(f"Data quality check failed for table {column} with zero records!")
    else:
        print(f"Data quality check passed for table {column} with {total_count} records.")
