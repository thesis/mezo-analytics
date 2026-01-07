from datetime import date, timedelta
import os

from dotenv import load_dotenv
import pandas as pd
import requests

from mezo.clients import BigQueryClient, SupabaseClient
from mezo.visual_utils import ProgressIndicators, with_progress

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Fetching token registrations from mezo_token_distributions table in Supabase")
def fetch_token_registrations(supabase):
    raw = supabase.fetch_table_data("mezo_token_distributions")

    return raw

@with_progress("Cleaning raw token registrations data")
def clean_token_registrations(raw):
    stg = raw.sort_values(by='updated_at', ascending=False)

    stg['updated_at'] = pd.to_datetime(stg['updated_at']).dt.date
    stg['created_at'] = pd.to_datetime(stg['created_at']).dt.date
    stg['terms_accepted_at'] = pd.to_datetime(stg['terms_accepted_at']).dt.date

    return stg

@with_progress("Loading environment variables")
def load_environment_variables():
    ProgressIndicators.print_step("Loading environment variables", "start")
    load_dotenv(dotenv_path="../.env", override=True)
    pd.options.display.float_format = "{:.8f}".format
    ProgressIndicators.print_step("Environment loaded successfully", "success")

@with_progress("Initializing database clients")
def initialize_database_clients(dev=False):
    ProgressIndicators.print_step("Initializing database clients", "start")

    if not dev:
        bq = BigQueryClient(key="GOOGLE_CLOUD_KEY", project_id="mezo-portal-data")
    elif dev:
        bq = BigQueryClient(key="GOOGLE_CLOUD_KEY_DEV", project_id="mezo-data-dev")

    ProgressIndicators.print_step("Database clients initialized", "success")

    return bq

@with_progress("Saving token registrations data to csv")
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

@with_progress("Printing summary statistics")
def print_summary(stg):
    df_all = stg.copy()
    df_today = stg[stg["updated_at"].astype(str) == date.today().strftime("%Y-%m-%d")]
    df_7d = stg[stg["updated_at"].astype(str) >= (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")]

    print(f"\n{'─' * 60}")
    print("TOKEN REGISTRATIONS SUMMARY \n")

    print(f"Total registrations today:      {df_today['address'].count():,}")
    print(f"Total registrations 7d:         {df_7d['address'].count():,}")
    print(f"Total registrations:            {df_all['address'].count():,}")
    print(f"Total liquid registrations:     {df_all[df_all['token_preference'] == 'liquid']['address'].count():,}")
    print(f"Total locked registrations:     {df_all[df_all['token_preference'] == 'locked']['address'].count():,}")
    
    print(f"{'─' * 60}\n")

@with_progress("Sending summary to Discord")
def send_discord_summary(stg, webhook_url):
    """
    Send token registration summary to Discord via webhook.
    
    Args:
        stg: DataFrame with token registration data
        webhook_url: Discord webhook URL
    """
    try:
        df_all = stg.copy()
        df_today = stg[stg["updated_at"].astype(str) == date.today().strftime("%Y-%m-%d")]
        df_7d = stg[stg["updated_at"].astype(str) >= (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")]
        
        total_today = df_today['address'].count()
        total_7d = df_7d['address'].count()
        total_all = df_all['address'].count()
        total_liquid = df_all[df_all['token_preference'] == 'liquid']['address'].count()
        total_locked = df_all[df_all['token_preference'] == 'locked']['address'].count()
        
        # Format numbers with commas
        def format_number(num):
            return f"{num:,}"
        
        # Create Discord embed
        embed = {
            "title": "📌 Token Registration Summary",
            "description": f"Daily update for {date.today().strftime('%B %d, %Y')}",
            "color": 3447003,  # Blue color
            "fields": [
                {
                    "name": "📅 Today",
                    "value": format_number(total_today),
                    "inline": True
                },
                {
                    "name": "📊 Last 7 Days",
                    "value": format_number(total_7d),
                    "inline": True
                },
                {
                    "name": "📈 Total",
                    "value": format_number(total_all),
                    "inline": True
                },
                {
                    "name": "💧 Liquid",
                    "value": format_number(total_liquid),
                    "inline": True
                },
                {
                    "name": "🔒 Locked",
                    "value": format_number(total_locked),
                    "inline": True
                }
            ],
            "timestamp": date.today().isoformat()
        }
        
        payload = {
            "embeds": [embed]
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        ProgressIndicators.print_step("Summary sent to Discord successfully", "success")
        
    except Exception as e:
        ProgressIndicators.print_step(f"Failed to send Discord summary: {str(e)}", "error")
        # Don't raise - allow script to continue even if Discord fails

# ==================================================
# MAIN
# ==================================================

def main(test_mode=False, skip_bigquery=False):

    ProgressIndicators.print_header("📌 TOKEN REGISTRATION PROCESSING PIPELINE")
    
    supabase = SupabaseClient(url="SUPABASE_URL_PROD", key="SUPABASE_KEY_PROD")

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        print(f"{'─' * 60}\n")
    
    if skip_bigquery:
        print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    try:
        load_environment_variables()
    
        if not skip_bigquery:
            bq = initialize_database_clients()

        if not test_mode:
            raw_data = fetch_token_registrations(supabase)
            save_to_csv(raw_data, 'raw_token_registrations')
        
        else:
            raw_data = pd.read_csv('raw_token_registrations.csv')

        if not skip_bigquery:
            raw_datasets = [
                (raw_data, 'raw_data', 'token_registrations_raw', 'id')
            ]

            for df, database, table_name, id_column in raw_datasets:
                upload_to_bigquery(df, database, table_name, id_column, bq)

        # ==================================================
        # CLEAN VOTING ESCROW DATA
        # ==================================================

        stg_data = clean_token_registrations(raw_data)

        if not test_mode:
            save_to_csv(stg_data, 'stg_token_registrations')

        if not skip_bigquery:
            stg_datasets = [    
                (stg_data, 'staging', 'token_registrations_stg', 'id')
            ]
            for df, database, table_name, id_column in stg_datasets:
                upload_to_bigquery(df, database, table_name, id_column, bq)

        ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")

        print_summary(stg_data)
        
        # Send summary to Discord
        webhook_url = "https://discord.com/api/webhooks/1458514561140523060/SNwJBsyXIiy5Jer-jAV2xruYQGObz4JHC2dzwvIozVM7BK1F1C31ca4eciEkp0vklEI5"
        send_discord_summary(stg_data, webhook_url)

    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header("❌ PROCESSING FAILED")

        print("\n📍 Error traceback:")
        print(f"{'─' * 50}")

        import traceback

        traceback.print_exc()
        print(f"{'─' * 50}")

        raise

if __name__ == "__main__":
    main()