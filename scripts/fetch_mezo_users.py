import pandas as pd
from mezo.clients import SupabaseClient, BigQueryClient
from mezo.visual_utils import with_progress, ProgressIndicators

supabase = SupabaseClient()
bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

@with_progress("Processing users with auth IDs")
def process_users_with_auth_id(df, last_active: None):
    df = df.sort_values(by='updated_at', ascending=False)
    df = df.loc[df['auth_user_id'].notna()].reset_index()

    if last_active is not None:
        df = df[df['updated_at'] >= last_active].reset_index()
        df['updated_at'] = pd.to_datetime(df['updated_at']).dt.date
    
    return df

def merge_and_clean_users(users, bridged_users):
    users_with_bridged_funds = pd.merge(bridged_users, users, how='left', on='address')
    
    users_with_bridged_funds = users_with_bridged_funds.sort_values(by='updated_at', ascending=False)
    users_with_bridged_funds['update_at'] = pd.to_datetime(users_with_bridged_funds['updated_at']).dt.date
    
    final_df = users_with_bridged_funds.loc[users_with_bridged_funds['updated_at'] >= '2025-07-01'].reset_index()

    return final_df


def main():
    ProgressIndicators.print_header("USER DATA PROCESSING")

    ProgressIndicators.print_step("Fetching Supabase tables", "start")
    users = supabase.fetch_table_data('accounts')
    bridged_users = supabase.fetch_table_data('accounts_with_bridged_funds')
    ProgressIndicators.print_step("Supabase tables loaded successfully", "success")

    ProgressIndicators.print_step("Cleaning user data", "start")
    all_users_with_auth_id = process_users_with_auth_id(users, None)
    raw_users = all_users_with_auth_id[['updated_at', 'address', 'evm_address', 'auth_user_id']]
    start = '2025-07-01'
    users_with_auth_id = process_users_with_auth_id(users, start)
    int_users = users_with_auth_id[['updated_at', 'address', 'evm_address', 'auth_user_id']]
    ProgressIndicators.print_step("User data cleaned", "success")

    ProgressIndicators.print_step("Uploading raw_users to BigQuery", "start")
    if raw_users is not None and len(raw_users) > 0:
        bq.update_table(raw_users, 'raw_data', 'mezo_users_raw', 'auth_user_id')
    ProgressIndicators.print_step("Successfully updated mezo_users_raw", "success")

    ProgressIndicators.print_step("Uploading int_users to BigQuery", "start")
    if int_users is not None and len(int_users) > 0:
        bq.update_table(int_users, 'intermediate', 'int_users', 'auth_user_id')
    ProgressIndicators.print_step("Successfully updated int_users", "success")

    total_users = users['address'].count()
    total_users_with_auth_ids = int_users['address'].count()
    all_time_users_with_auth_ids = raw_users['address'].count()
    
    ProgressIndicators.print_summary_box(
        "📊 USERS SUMMARY",
        {
            "Starting date": start,
            "Total users": total_users,
            "All users with auth IDs": all_time_users_with_auth_ids,
            "Total users with auth IDs (from start date)": total_users_with_auth_ids
        }
    )

    ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")

if __name__ == "__main__":
    results = main()