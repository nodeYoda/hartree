"""Hartree Partners take home test"""

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam import dataframe
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_pcollection


def add_max_rating(df):

    max_rating = df[['counter_party', 'rating']].groupby('counter_party').max().reset_index()
    max_rating = max_rating.rename(columns={'rating': 'max(rating by counterparty)'})
    df = df.merge(max_rating, how='left', on='counter_party')
    return df


def add_sum_status(df):

    sum_df = df[['status', 'value']].groupby('status').sum()
    for status in ['ARAP', 'ACCR']:
        col = f'sum(value where status={status})'
        df[col] = np.where(df['status'] == status, sum_df.at[status, 'value'], pd.NA)

    return df

def using_pandas():

    df1 = pd.read_csv('dataset1.csv', index_col='counter_party')
    df2 = pd.read_csv('dataset2.csv', index_col='counter_party')
    df = df1.join(df2).reset_index()
    df = add_max_rating(df)
    df = add_sum_status(df)
    df.to_csv('output_file_pandas.csv', index=False)

    gb_cols = ['legal_entity', 'counter_party', 'tier']
    new_record = df[gb_cols + ['value']].groupby(gb_cols).sum('value').reset_index()
    new_record.to_csv('new_record_pandas.csv', index=False)


def using_apache_beam():

    with beam.Pipeline() as p:
        with dataframe.allow_non_parallel_operations():
            df1 = p | "Read dataset1" >> read_csv('dataset1.csv', index_col='counter_party')
            df2 = p | "Read dataset2" >> read_csv('dataset2.csv', index_col='counter_party')
            df = df1.join(df2).reset_index()
            df = add_max_rating(df)

if __name__ == '__main__':
    using_pandas()
    using_apache_beam()
