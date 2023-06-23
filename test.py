"""
Hartree Partners take home test

Please do same exercise using two different framework.

Framework 1. pandas
framework 2. apache beam python https://beam.apache.org/documentation/sdks/python/


using two input files dataset1 and dataset2 

join dataset1 with dataset2 and get tier

generate below output file

legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)

Also create new record to add total for each of legal entity, counterparty & tier.

Sample data:
legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
L1,Total, Total, calculated_value, calculated_value,calculated_value
L1, C1, Total,calculated_value, calculated_value,calculated_value
Total,C1,Total,calculated_value, calculated_value,calculated_value
Total,Total,1,calculated_value, calculated_value,calculated_value
L2,Total,Total,calculated_value, calculated_value,calculated_value
....
like all other values.

where caluclated_value in sample data needs to be calculated using above method.
"""

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

if __name__ == '__main__':
    main()
