import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from mezo.currency_utils import format_musd_currency_columns
from mezo.datetime_utils import format_datetimes
from mezo.clients import BigQueryClient, SubgraphClient, Web3Client
from mezo.queries import VaultQueries
from mezo.visual_utils import ProgressIndicators

load_dotenv(dotenv_path='../.env', override=True)
COINGECKO_KEY = os.getenv('COINGECKO_KEY')

vaults = SubgraphClient.get_subgraph_data(SubgraphClient.AUGUST_VAULT_SUBGRAPH, VaultQueries.GET_VAULT_TRANSFERS, 'transfers')
vaults_df = vaults.copy()

vaults_df['value'] = vaults_df['value'].astype('float')
vaults_df = format_musd_currency_columns(vaults_df, ['value'])
vaults_df = format_datetimes(vaults_df, ['timestamp_'])
vaults_df['type'] = 'transfer'  # default value
vaults_df.loc[vaults_df['from'] == '0x0000000000000000000000000000000000000000', 'type'] = 'deposit' 
vaults_df.loc[vaults_df['to'] == '0x0000000000000000000000000000000000000000', 'type'] = 'withdrawal'

daily_vault_txns = vaults_df.groupby(['timestamp_']).agg(
    total_value = ('value', 'sum'),
    total_transactions = ('transactionHash_', 'count'),
    deposit_count = ('type', lambda x: (x == 'deposit').sum()),
    deposit_amt = ('value', lambda x: x.where(vaults_df.loc[x.index, 'type'] == 'deposit').sum()),
    withdrawal_count = ('type', lambda x: (x == 'withdrawal').sum()),
    withdrawal_amt = ('value', lambda x: x.where(vaults_df.loc[x.index, 'type'] ==
'withdrawal').sum()),
    unique_users = ('to', lambda x: x.nunique())
).reset_index().sort_values(by='timestamp_', ascending=False)
daily_vault_txns = daily_vault_txns.fillna(0)

bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

ProgressIndicators.print_step("Uploading raw vaults data to BigQuery", "start")
if vaults is not None and len(vaults) > 0:
    bq.update_table(vaults, 'raw_data', 'vaults_raw', 'transactionHash_')
    ProgressIndicators.print_step("Uploaded raw vaults to BigQuery", "success")

ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
datasets_to_upload = [
    (vaults_df, 'vaults_clean', 'transactionHash_'),
    (daily_vault_txns, 'daily_vaults_clean', 'timestamp_')
]
for dataset, table_name, id_column in datasets_to_upload:
    if dataset is not None and len(dataset) > 0:
        bq.update_table(dataset, 'staging', table_name, id_column)
        ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")