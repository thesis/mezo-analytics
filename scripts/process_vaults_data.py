import pandas as pd

from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_utils import Conversions
from mezo.data_utils import add_cumulative_columns, add_rolling_values
from mezo.datetime_utils import format_datetimes
from mezo.queries import VaultQueries

# from mezo.test_utils import tests
from mezo.visual_utils import ProgressIndicators

# ========================================
# helper functions
# ========================================

def process_vaults_data(df):
    conversions = Conversions()
    vaults_df = df.copy()
    vaults_df['value'] = vaults_df['value'].astype('float')
    vaults_df = conversions.format_token_decimals(vaults_df, amount_cols=['value'])
    # vaults_df = format_musd_currency_columns(vaults_df, ['value'])
    vaults_df = format_datetimes(vaults_df, ['timestamp_'])
    vaults_df['type'] = 'transfer'  # default value
    vaults_df.loc[vaults_df['from'] == '0x0000000000000000000000000000000000000000', 'type'] = 'deposit' 
    vaults_df.loc[vaults_df['to'] == '0x0000000000000000000000000000000000000000', 'type'] = 'withdrawal'

    df.to_csv('vaults_clean.csv')

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
    
    daily_vault_txns = add_rolling_values(
        daily_vault_txns, 30, [
            'TVL', 'volume', 'daily_flow', 
            'deposit_amt', 'withdrawal_amt'
        ]
    ).fillna(0)
    
    daily_vault_txns = add_cumulative_columns(
        daily_vault_txns, [
            'volume', 'deposit_amt', 'withdrawal_amt', 
            'total_transactions', 'deposit_count', 
            'withdrawal_count', 'unique_users'
        ]
    ).fillna(0)

    return daily_vault_txns

# ========================================
# main process
# ========================================

def main(test_mode=False, sample_size=False, skip_bigquery=False):

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        if sample_size:
            print(f"{'Using sample size: ' + str(sample_size):^60}")
        if skip_bigquery:
            print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    if not skip_bigquery:
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

    # ========================================
    # fetch raw data from august vaults subgraph
    # ========================================

    if not test_mode:
        vaults = SubgraphClient.get_subgraph_data(
            SubgraphClient.AUGUST_VAULT_SUBGRAPH, 
            VaultQueries.GET_VAULT_TRANSFERS, 
            'transfers'
        )
        vaults.to_csv('raw_vaults_data.csv')
    else:
        vaults = pd.read_csv('raw_vaults_data.csv')        

    # ========================================
    # clean and process raw vaults data
    # ========================================
    
    vaults_df = process_vaults_data(vaults)
    daily_vault_txns = aggregate_vaults_by_day(vaults_df)

    # ========================================
    # upload vaults data to BigQuery
    # ========================================

    if not skip_bigquery:
        ProgressIndicators.print_step("Uploading raw vaults data to BigQuery", "start")
        if vaults is not None and len(vaults) > 0:
            bq.update_table(vaults, 'raw_data', 'vaults_raw', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded raw vaults to BigQuery", "success")

        ProgressIndicators.print_step("Uploading clean vaults data to BigQuery", "start")
        if vaults_df is not None and len(vaults_df) > 0:
            bq.update_table(vaults_df, 'staging', 'vaults_clean', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded clean vaults data to BigQuery", "success")

        ProgressIndicators.print_step("Uploading aggregate data to BigQuery", "start")
        datasets_to_upload = [
            (daily_vault_txns, 'agg_daily_vaults', 'timestamp_')
        ]

        for dataset, table_name, id_column in datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'marts', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

if __name__ == "__main__":
    results = main()

    # test = tests()
    # for testing, uncomment one of these:
        # results = quick_test(sample_size=500)
        # test.inspect_data(results)
        # test.save_test_outputs(results)