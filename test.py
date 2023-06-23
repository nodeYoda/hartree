"""Hartree Partners take home test"""

import numpy as np
import pandas as pd


def main():

    df1 = pd.read_csv('dataset1.csv', index_col='counter_party')
    df2 = pd.read_csv('dataset2.csv', index_col='counter_party')
    df = df1.join(df2).reset_index()

    max_rating = df[['counter_party', 'rating']].groupby('counter_party').max().reset_index()
    max_rating = max_rating.rename(columns={'rating': 'max(rating by counterparty)'})
    df = df.merge(max_rating, how='left', on='counter_party')

    sum_df = df[['status', 'value']].groupby('status').sum().reset_index()
    for status in ['ARAP', 'ACCR']:
        col = f'sum(value where status={status})'
        df[col] = np.where(df['status'] == status, sum_df.loc[sum_df['status'] == status]['value'].values[0], pd.NA)

    df.to_csv('output_file.csv', index=False)

    gb_cols = ['legal_entity', 'counter_party', 'tier']
    new_record = df[gb_cols + ['value']].groupby(gb_cols).sum('value').reset_index()
    new_record.to_csv('new_record.csv', index=False)

if __name__ == '__main__':
    main()
