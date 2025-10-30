import pandas as pd

from mezo.clients import BigQueryClient, SubgraphClient, SupabaseClient
from mezo.currency_utils import Conversions
from mezo.data_utils import flatten_json_column
from mezo.datetime_utils import format_datetimes
from mezo.queries import BridgeQueries
from mezo.visual_utils import ProgressIndicators

# ==================================================
# HELPER FUNCTIONS
# ==================================================

def clean_lolli_subgraph_data(raw, sort_col, date_cols, currency_cols, conversions):
    """Clean and format Lolli wit data."""

    df = raw.copy().sort_values(by=sort_col)
    df = format_datetimes(df, date_cols)
    df = conversions.format_token_decimals(df, currency_cols)

    mapping = {
    'timestamp_': 'date', 
    'value': 'amount', 
    'transactionHash_': 'transaction_hash'
    }

    df_int = df.rename(columns=mapping)

    return df_int

def clean_lolli_supabase_data(df):
    """Clean and format Lolli user data."""
    df_stg = df[
        [
            "created_at",
            "email",
            "uuid",
            "country_code",
            "preferred_currency",
            "display_currency",
            "sats_tag",
            "last_accepted_terms",
            "last_accepted_privacy_policy",
            "account_status",
            "withdrawal_addresses",
            "mezo_connection",
        ]
    ]

    df_norm = flatten_json_column(df_stg, "mezo_connection")
    df_int = flatten_json_column(df_norm, "withdrawal_addresses")
    date_cols = ["created_at", "last_accepted_terms", "last_accepted_privacy_policy"]
    for col in date_cols:
        df_int[col] = pd.to_datetime(df_int[col], format="ISO8601").dt.date

    return df_int

def upload_lolli_to_supabase(bq, df, database, table, identifier):
    ProgressIndicators.print_step(f"Uploading {table} to BigQuery db {database}", "start")
    if df is not None and len(df) > 0:
        bq.update_table(df, database, table, identifier)
    ProgressIndicators.print_step(f"Successfully updated {table} table", "success")

# ==================================================
# MAIN FUNCTION
# ==================================================

def main():
    sb = SupabaseClient(key='LOLLI_SB_PROD_KEY', url='LOLLI_SB_PROD_URL')
    bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
    conversions = Conversions()

    # loads lolli withdrawal data from the subgraph, cleans it, and returns the cleaned df for bigquery upload
    lolli_raw = SubgraphClient.get_subgraph_data(
        SubgraphClient.LOLLI_WIT_SUBGRAPH,
        BridgeQueries.GET_LOLLI_WITHDRAWALS,
        'transfers'
    )
    lolli_int = clean_lolli_subgraph_data(lolli_raw, 'timestamp_', ['timestamp_'], ['value'], conversions).reset_index(drop=True)

    # loads lolli 'users' table from supabase, cleans it, and returns the cleaned df for bigquery upload
    lolli_users = sb.fetch_table_data('users')
    lolli_users_int = clean_lolli_supabase_data(lolli_users)

    # uploads the two df's to mezo's bigquery under the "lolli" database
    upload_lolli_to_supabase(bq, lolli_int, "lolli", "int_lolli_withdrawals", "transaction_hash")
    upload_lolli_to_supabase(bq, lolli_users_int, "lolli", "int_lolli_users", "uuid")

    ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")

if __name__ == "__main__":
    main()