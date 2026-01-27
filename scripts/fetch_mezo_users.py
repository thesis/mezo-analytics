from datetime import date, timedelta
import os

from dotenv import load_dotenv
import pandas as pd

from mezo.clients import BigQueryClient, SupabaseClient
from mezo.visual_utils import ProgressIndicators, with_progress

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Cleaning raw user data")
def clean_users(df, last_active=None):
    df = df.sort_values(by='updated_at', ascending=False)
    df = df.loc[df['auth_user_id'].notna()].reset_index()

    if last_active is not None:
        df = df[df['updated_at'] >= last_active].reset_index()
        df['updated_at'] = pd.to_datetime(df['updated_at']).dt.date
    
    df = df[['updated_at', 'address', 'evm_address', 'auth_user_id', 'has_modified_username', 'metadata']]
    
    return df

@with_progress("Fetching user data from accounts table in Supabase")
def fetch_users(supabase):
    users = supabase.fetch_table_data('accounts')

    return users

@with_progress("Saving user data to csv")
def save_to_csv(df, name):
    os.makedirs('./outputs', exist_ok=True)
    
    yesterday = date.today() - timedelta(days=1)
    previous_day_path = f'./outputs/{name}_{yesterday}.csv'

    if os.path.exists(previous_day_path):
        os.remove(previous_day_path)
        print(f"Deleted previous day's CSV: {previous_day_path}")
    
    output_path = f'./outputs/{name}_{date.today()}.csv'
    
    df.to_csv(output_path)

@with_progress("Uploading data to BigQuery")
def upload_to_bigquery(df, dataset, table, identifier, bq):
    if df is not None and len(df) > 0:
        bq.update_table(df, dataset, table, identifier)

@with_progress("Creating Galxe export")
def create_galxe_export(df, name):
    df = df[["address"]].reset_index(drop=True)
    save_to_csv(df, name)

def get_btc_users(users):
    btc_users = users[users["address"].str.startswith("b", na=False)].reset_index()

    return btc_users

def print_summary(users, start):
    total_users = users['address'].count()
    
    ProgressIndicators.print_summary_box(
        "📊 USERS SUMMARY",
        {
            "Starting date": start,
            "Total users": f"{total_users:,}"
        }
    )

# ==================================================
# RUN MAIN FUNCTION
# ==================================================

def main(skip_bigquery=False, test_mode=False):
    ProgressIndicators.print_header("📌 GET MEZO USER DATA")    

    # set up env and clients
    load_dotenv(dotenv_path='../.env', override=True)
    supabase = SupabaseClient(url='SUPABASE_URL_PROD', key='SUPABASE_KEY_PROD')    
    bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

    # set start date for filtering users
    start = '2025-05-28' # mainnet launch

    # fetch raw data from supabase `accounts` table
    users_raw = fetch_users(supabase)
    create_galxe_export(users_raw, "users_raw")

    # clean the raw users data and create staging tables    
    users_stg = clean_users(users_raw, start)
    save_to_csv(users_stg, 'users')

    # bigquery data uploads
    upload_to_bigquery(users_raw, 'supabase', 'raw_mezo_users', 'auth_user_id', bq)
    upload_to_bigquery(users_stg, 'staging', 'mezo_users_stg', 'auth_user_id', bq)

    print_summary(users_raw, start)
    
    ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")

if __name__ == "__main__":
    main()