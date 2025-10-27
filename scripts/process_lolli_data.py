import pandas as pd
from mezo.clients import SupabaseClient, BigQueryClient, SubgraphClient
from mezo.visual_utils import ProgressIndicators
from mezo.queries import BridgeQueries
from mezo.currency_utils import Conversions
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import flatten_json_column
# ==================================================
# HELPER FUNCTIONS
# ==================================================

def clean_lolli_data(raw, sort_col, date_cols, currency_cols):
    """Clean and format Lolli wit data."""
    conversions = Conversions()

    df = raw.copy().sort_values(by=sort_col)
    df = format_datetimes(df, date_cols)
    df = conversions.format_token_decimals(df, currency_cols)

    return df

# ==================================================
# RUN MAIN FUNCTION
# ==================================================

sb = SupabaseClient(key='LOLLI_SB_PROD_KEY', url='LOLLI_SB_PROD_URL')
bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

lolli_raw = SubgraphClient.get_subgraph_data(
    SubgraphClient.LOLLI_WIT_SUBGRAPH,
    BridgeQueries.GET_LOLLI_WITHDRAWALS,
    'transfers'
)

lolli_raw['value']

lolli_stg = clean_lolli_data(lolli_raw, 'timestamp_', ['timestamp_'], ['value']).reset_index(drop=True)

mapping = {
    'timestamp_': 'date', 
    'value': 'amount', 
    'transactionHash_': 'transaction_hash'
}

lolli_int = lolli_stg.rename(columns=mapping)

# ==================================================
# LOAD AND CLEAN DATA FROM SUPABASE
# ==================================================

lolli_users = sb.fetch_table_data('users')

lolli_users_stg = lolli_users[[
    'created_at', 'email', 'uuid', 'country_code', 
    'preferred_currency', 'display_currency', 'sats_tag', 
    'last_accepted_terms', 'last_accepted_privacy_policy',
    'account_status', 'withdrawal_addresses', 'mezo_connection'
]]

lolli_norm = flatten_json_column(lolli_users_stg, 'mezo_connection')
lolli_norm.columns
lolli_users_int = flatten_json_column(lolli_norm, 'withdrawal_addresses')

date_cols = ['created_at', 'last_accepted_terms', 'last_accepted_privacy_policy']

for col in date_cols:
    lolli_users_int[col] = pd.to_datetime(
        lolli_users_int[col], format='ISO8601'
    ).dt.date

# ==================================================
# UPLOAD TO BIGQUERY
# ==================================================

ProgressIndicators.print_step("Uploading lolli withdrawals to BigQuery", "start")
if lolli_int is not None and len(lolli_int) > 0:
    bq.update_table(lolli_int, 'lolli', 'int_lolli_withdrawals', 'transaction_hash')
ProgressIndicators.print_step("Successfully updated lolli withdrawals table", "success")

ProgressIndicators.print_step("Uploading lolli user data to BigQuery", "start")
if lolli_users_int is not None and len(lolli_users_int) > 0:
    bq.update_table(lolli_users_int, 'lolli', 'int_lolli_users', 'uuid')
ProgressIndicators.print_step("Successfully updated lolli users table", "success")


ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")