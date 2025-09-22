import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from mezo.currency_utils import format_musd_currency_columns
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_cumulative_columns, add_rolling_values
from mezo.clients import BigQueryClient, SubgraphClient, Web3Client
from mezo.queries import VaultQueries
from mezo.visual_utils import ProgressIndicators

def process_vaults_data(df):
    vaults_df = df.copy()
    vaults_df['value'] = vaults_df['value'].astype('float')
    vaults_df = format_musd_currency_columns(vaults_df, ['value'])
    vaults_df = format_datetimes(vaults_df, ['timestamp_'])
    vaults_df['type'] = 'transfer'  # default value
    vaults_df.loc[vaults_df['from'] == '0x0000000000000000000000000000000000000000', 'type'] = 'deposit' 
    vaults_df.loc[vaults_df['to'] == '0x0000000000000000000000000000000000000000', 'type'] = 'withdrawal'

    return vaults_df

def aggregate_vaults_by_day(df):
    daily_vault_txns = df.groupby(['timestamp_']).agg(
        volume = ('value', 'sum'),
        total_transactions = ('transactionHash_', 'count'),
        deposit_count = ('type', lambda x: (x == 'deposit').sum()),
        deposit_amt = ('value', lambda x: x.where(df.loc[x.index, 'type'] == 'deposit').sum()),
        withdrawal_count = ('type', lambda x: (x == 'withdrawal').sum()),
        withdrawal_amt = ('value', lambda x: x.where(df.loc[x.index, 'type'] == 'withdrawal').sum()),
        unique_users = ('to', lambda x: x.nunique())
    ).sort_values(by='timestamp_').fillna(0).reset_index()

    daily_vault_txns['daily_flow'] = daily_vault_txns['deposit_amt'] - daily_vault_txns['withdrawal_amt']
    daily_vault_txns['TVL'] = daily_vault_txns['daily_flow'].cumsum()
    daily_vault_txns = add_rolling_values(daily_vault_txns, 30, ['TVL', 'volume', 'daily_flow', 'deposit_amt', 'withdrawal_amt'])

    return daily_vault_txns

def aggregate_cumulative_daily_vaults(df_daily):
    cumul_daily_vault_txns = add_cumulative_columns(df_daily, ['deposit_amt', 'withdrawal_amt', 'total_transactions', 'deposit_count', 'withdrawal_count', 'unique_users'])

    cumul_daily_vault_txns = cumul_daily_vault_txns[[
        'timestamp_', 'TVL', 'cumulative_deposit_amt', 'cumulative_withdrawal_amt', 
        'cumulative_total_transactions', 'cumulative_deposit_count', 
        'cumulative_withdrawal_count', 'cumulative_unique_users', 
        'cumulative_deposit_amt_growth', 'cumulative_withdrawal_amt_growth', 'cumulative_total_transactions_growth',
        'cumulative_deposit_count_growth', 'cumulative_withdrawal_count_growth', 'cumulative_unique_users_growth'
    ]]

    return cumul_daily_vault_txns

def main():
    # Initialize environment
    load_dotenv(dotenv_path='../.env', override=True)
    COINGECKO_KEY = os.getenv('COINGECKO_KEY')

    # Initialize BigQuery client
    bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

    # Get vaults data from august vault subgraph
    vaults = SubgraphClient.get_subgraph_data(SubgraphClient.AUGUST_VAULT_SUBGRAPH, VaultQueries.GET_VAULT_TRANSFERS, 'transfers')

    # Clean raw vaults data
    vaults_df = process_vaults_data(vaults)

    # Aggregate vaults data by day
    daily_vault_txns = aggregate_vaults_by_day(vaults_df)
    cumul_daily_vault_txns = aggregate_cumulative_daily_vaults(daily_vault_txns)

    # Upload raw vaults data to BigQuery
    ProgressIndicators.print_step("Uploading raw vaults data to BigQuery", "start")
    if vaults is not None and len(vaults) > 0:
        bq.update_table(vaults, 'raw_data', 'vaults_raw', 'transactionHash_')
        ProgressIndicators.print_step("Uploaded raw vaults to BigQuery", "success")

    # Upload clean vaults data to BigQuery
    ProgressIndicators.print_step("Uploading clean vaults data to BigQuery", "start")
    if vaults_df is not None and len(vaults_df) > 0:
        bq.update_table(vaults_df, 'staging', 'vaults_clean', 'transactionHash_')
        ProgressIndicators.print_step("Uploaded clean vaults data to BigQuery", "success")

    # Upload aggregate vaults data to BigQuery
    ProgressIndicators.print_step("Uploading aggregate data to BigQuery", "start")
    datasets_to_upload = [
        (daily_vault_txns, 'agg_daily_vaults', 'timestamp_'),
        (cumul_daily_vault_txns, 'agg_cumulative_daily_vaults', 'timestamp_')
    ]

    for dataset, table_name, id_column in datasets_to_upload:
        if dataset is not None and len(dataset) > 0:
            bq.update_table(dataset, 'marts', table_name, id_column)
            ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

if __name__ == "__main__":
    results = main()