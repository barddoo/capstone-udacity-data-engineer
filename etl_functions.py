import os.path

import bovespa
import pandas as pd
import wbgapi as wb
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


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


def create_economy_pandas():
    """
    Load data from world bank
    :return: pandas dataframe representing economy table
    """
    metric_to_schema = {
        'GC.DOD.TOTL.GD.ZS': 'debt',
        'GC.XPN.TOTL.CN': 'total_expense',
        'NY.GDP.MKTP.KD.ZG': 'gdp_annual_growth',
        'NY.GDP.MKTP.CD': 'gdp',
        'NY.GDP.PCAP.CD': 'gdp_per_capita',
        'SP.POP.TOTL': 'population',
        'SP.DYN.LE00.IN': 'life_expectancy',
        'GC.XPN.TOTL.GD.ZS': 'expense_per_gdp',
        'FI.RES.TOTL.CD': 'total_reserves',
        'SE.ADT.LITR.ZS': 'pop_literacy_rate',
        'SE.XPD.TOTL.GD.ZS': 'expenditure_education_per_gdp',
        'BX.KLT.DINV.CD.WD': 'foreign_investment',
    }
    country = 'BRA'
    values = []
    for key, value in metric_to_schema.items():
        df = wb.data.DataFrame(key, economy=country, columns='time', numericTimeKeys=True).dropna(axis=1, how='all')
        for data in df.items():
            values.append((data[0], value, data[1][country]))
    return pd.DataFrame(data=values, columns=['year', 'table', 'value'])


def raw_trading_to_pandas(raw_df):
    """
    Convert dataframe to pandas, transform position string into bovespa.Record, transform it into a dict then
    explode the columns to the schema.
    :param raw_df: (spark dataframe)
    :return: pandas dataframe
    """
    raw_df = raw_df.toPandas()
    raw_df['dict'] = raw_df['value'].apply(lambda x: record_to_dict(x))
    raw_df = raw_df['dict'].apply(pd.Series)
    raw_df.replace(r"^\s*$", float("NaN"), regex=True, inplace=True)
    raw_df['money_volume'] = pd.to_numeric(raw_df['money_volume'], errors='coerce')
    raw_df['volume'] = pd.to_numeric(raw_df['volume'], errors='coerce', downcast='integer')
    raw_df['variation'] = (raw_df['price_close'] / raw_df['price_open']) / 100
    return raw_df.dropna()


def trading_pandas_to_spark(spark, pd_df):
    """
    Load pandas dataframe into spark.
    :param spark: sparkSession
    :param pd_df: pandas dataframe
    :return: spark dataframe with schema.
    """
    schema = StructType([
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
    return spark.createDataFrame(pd_df, schema=schema, verifySchema=False)


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
def quality_checks(df, table_name):
    """Count checks on fact and dimension table to ensure completeness of data.
    :param df: spark dataframe to check counts on
    :param table_name: corresponding name of table
    """

    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {table_name} with zero records!")
    else:
        print(f"Data quality check passed for {table_name} with {total_count:,} records.")
    return 0
