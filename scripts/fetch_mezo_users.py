from datetime import date
from dotenv import load_dotenv
import pandas as pd
from mezo.clients import SupabaseClient, BigQueryClient
from mezo.visual_utils import with_progress, ProgressIndicators

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Cleaning user table")
def clean_users(df, last_active: None):
    df = df.sort_values(by='updated_at', ascending=False)
    df = df.loc[df['auth_user_id'].notna()].reset_index()

    if last_active is not None:
        df = df[df['updated_at'] >= last_active].reset_index()
        df['updated_at'] = pd.to_datetime(df['updated_at']).dt.date
    
    df = df[['updated_at', 'address', 'evm_address', 'auth_user_id', 'has_modified_username', 'metadata']]
    
    return df

@with_progress("Fetching table from Supabase")
def fetch_users(supabase):
    users = supabase.fetch_table_data('accounts')

    return users

def fetch_btc_users(users):
    btc_users = users[users['address'].str.startswith('b', na=False)].reset_index()
    return btc_users

@with_progress("Saving to csv")
def save_to_csv(df, name):
    df.to_csv(f'/Users/laurenjackson/Desktop/mezo-analytics/outputs/{name}_{date.today()}.csv')

@with_progress("Uploading raw_users to BigQuery")
def upload_to_bigquery(df, dataset, table, identifier, bq):
    if df is not None and len(df) > 0:
        bq.update_table(df, dataset, table, identifier)

def print_summary(users, start):
    total_users = users['address'].count()
    # total_users_with_auth_ids = int_users['address'].count()
    # all_time_users_with_auth_ids = raw_users['address'].count()
    
    ProgressIndicators.print_summary_box(
        "📊 USERS SUMMARY",
        {
            "Starting date": start,
            "Total users": total_users,
        }
    )

# ==================================================
# RUN MAIN FUNCTION
# ==================================================

def main():
    ProgressIndicators.print_header("📌 GET MEZO USER DATA")    
    load_dotenv(dotenv_path='../.env', override=True)
    supabase = SupabaseClient(url='SUPABASE_URL_PROD', key='SUPABASE_KEY_PROD')    
    bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

    # ==================================================
    # LOAD AND CLEAN DATA FROM SUPABASE
    # ==================================================
    start = '2025-05-28' # mainnet launch

    users = fetch_users(supabase)
    
    users_stg = clean_users(users, start)
    save_to_csv(users_stg, 'users')

    btc_users = fetch_btc_users(users)
    save_to_csv(btc_users, 'btc_users')

    # ==================================================
    # UPLOAD TO BIGQUERY
    # ==================================================
    upload_to_bigquery(users_stg, 'staging', 'mezo_users_stg', 'auth_user_id')

    # ==================================================
    # PRINT SUMMARY
    # ==================================================
    print_summary(users, start)
    
    ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")


if __name__ == "__main__":
    results = main()