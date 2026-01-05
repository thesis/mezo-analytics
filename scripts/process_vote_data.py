from datetime import date, timedelta
import os

from dotenv import load_dotenv
import pandas as pd

from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_config import MEZO_TOKEN_ADDRESSES
from mezo.currency_utils import Conversions
from mezo.data_utils import flatten_json_column
from mezo.datetime_utils import format_datetimes
from mezo.queries import VotingEscrowQueries
from mezo.visual_utils import ProgressIndicators, with_progress


@with_progress("Fetching voting escrow data from subgraph")
def fetch_voting_escrow_data(name: str, query: str, query_key: str) -> pd.DataFrame:
    ProgressIndicators.print_step("Fetching voting escrow data from subgraph", "start")

    raw = SubgraphClient.get_subgraph_data(
        name,
        query,
        query_key
    )

    ProgressIndicators.print_step(f"Loaded {len(raw):,} raw votes", "success")

    return raw

@with_progress("Cleaning voting escrow data")
def clean_voting_escrow_data(raw: pd.DataFrame) -> pd.DataFrame:
    ProgressIndicators.print_step("Cleaning voting escrow data", "start")

    df = raw.copy()
    df = flatten_json_column(df, 'staker')
    
    # handle date formatting
    df = format_datetimes(df, ['initializedAt', 'unlockAt', 'withdrawnAt'])
    
    # remove null date in the first row  of df
    df = df.dropna(subset=["initializedAt"])

    df[["lockDuration", "selectedLockDuration"]] = (
        df[["lockDuration", "selectedLockDuration"]].fillna(0).astype(int)
    )

    df["lockDuration_days"] = pd.to_timedelta(df["lockDuration"], unit="s")
    df["lockDuration_days"] = df["lockDuration_days"].astype(str)
    df["selectedLockDuration_days"] = pd.to_timedelta(df["selectedLockDuration"], unit="s")
    df["selectedLockDuration_days"] = df["selectedLockDuration_days"].astype(str)

    date_cols = ["initializedAt", "unlockAt", "withdrawnAt"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # handle token formatting
    df['token_address'] = df['token']
    conv = Conversions()

    # TO GET CONVERSION DATA, LOOK AT PYTHON SCRIPT FOR $MEZO (what about ve tokens/ veBTC?)
    df = conv.replace_token_addresses_with_symbols(df, 'token', MEZO_TOKEN_ADDRESSES)

    # cant add usd cols until we have a price oracle for $MEZO
    # df['amount_usd'] = conv.convert_token_to_usd(df['amount'], df['token'])
    # df['total_earned_usd'] = conv.convert_token_to_usd(df['total_earned'], df['token'])
    df = conv.format_token_decimals(df, ['amount', 'totalEarned'], 'token')

    col_map = {
        'id': 'txn_id',
        'initializedAt': 'date_staked',
        'amount': 'amount',
        'token': 'token',
        'selectedLockDuration': 'selected_lock_duration_seconds',
        'selectedLockDuration_days': 'selected_lock_duration',
        'lockDuration': 'lock_duration_seconds',
        'lockDuration_days': 'lock_duration',
        'isPermanent': 'is_permanent',
        'unlockAt': 'unlock_date',
        'totalEarned': 'total_earned',
        'isWithdrawn': 'is_withdrawn',
        'withdrawnAt': 'date_withdrawn',
        'withdrawnAmount': 'amount_withdrawn',
        'staker_id': 'wallet_address'
    }
    df.rename(columns=col_map, inplace=True)

    final_df = df[[
            "date_staked",
            "amount",
            "token",
            "selected_lock_duration",
            "selected_lock_duration_seconds",
            "lock_duration",
            "lock_duration_seconds",
            "is_permanent",
            "unlock_date",
            "total_earned",
            "is_withdrawn",
            "date_withdrawn",
            "amount_withdrawn",
            "wallet_address",
            "txn_id",
        ]]

    return final_df

@with_progress("Uploading voting escrow data to BigQuery")
def upload_to_bigquery(bq, df, database, table, identifier):
    print(f"📤 Uploading {len(df)} rows to {database}.{table}...")
    if df is not None and len(df) > 0:
        bq.update_table(df, database, table, identifier)
    print(f"📦 Uploaded {len(df)} rows to {database}.{table}")

@with_progress("Loading environment variables")
def load_environment_variables():
    ProgressIndicators.print_step("Loading environment variables", "start")
    load_dotenv(dotenv_path='../.env', override=True)
    pd.options.display.float_format = '{:.8f}'.format
    ProgressIndicators.print_step("Environment loaded successfully", "success")

@with_progress("Initializing database clients")
def initialize_database_clients(dev=False):
    ProgressIndicators.print_step("Initializing database clients", "start")
    
    if not dev:
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
    elif dev:
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY_DEV', project_id='mezo-data-dev')
    
    ProgressIndicators.print_step("Database clients initialized", "success")
    
    return bq

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

@with_progress("Getting summary stake and vote statistics")
def print_summary_stake_and_vote_statistics(df):
    
    # today stats
    df_today = df[df["date_staked"].astype(str) == date.today().strftime("%Y-%m-%d")]
    
    print(f"\n{'─' * 60}")
    print(f"{date.today()}: STAKE & VOTE SUMMARY \n")
    
    print(f"veBTC staked today:              {df_today[df_today['token'] == 'veBTC']['amount'].sum():,.6f}")
    print(f"veMEZO staked today:             {df_today[df_today['token'] == 'veMEZO']['amount'].sum():,.6f}")
    print(f"Total stakes today:              {df_today['wallet_address'].count():,}")
    print(f"Total stakers today:             {df_today['wallet_address'].nunique():,}")
    print(f"Permanent locks today:           {df_today[df_today['is_permanent'] == True]['wallet_address'].count():,}")
    print(f"Total veBTC earned today:        {df_today[df_today['token'] == 'veBTC']['total_earned'].sum():,.6f}")
    print(f"Total veMEZO earned today:       {df_today[df_today['token'] == 'veMEZO']['total_earned'].sum():,.6f}")
    
    print(f"{'─' * 60}\n")

    # epoch stats
    epoch_0_start = date(2025, 12, 11)  # Dec 11, 2025 - Thursday
    today = date.today()
    
    # Calculate current epoch number
    days_since_epoch_0 = (today - epoch_0_start).days
    current_epoch = days_since_epoch_0 // 7
    
    # Calculate current epoch start and end dates (Thursday to Thursday)
    epoch_start = epoch_0_start + timedelta(days=current_epoch * 7)
    epoch_end = epoch_start + timedelta(days=6)  # End of epoch (Wednesday)
    
    # Filter data for current epoch
    df_epoch = df[
        (df["date_staked"].astype(str) >= epoch_start.strftime("%Y-%m-%d")) &
        (df["date_staked"].astype(str) <= epoch_end.strftime("%Y-%m-%d"))
    ]
    
    print(f"\n{'─' * 60}")
    print(f"EPOCH {current_epoch} STAKE & VOTE SUMMARY ({epoch_start} to {epoch_end}) \n")
    
    print(f"veBTC staked epoch {current_epoch}:        {df_epoch[df_epoch['token'] == 'veBTC']['amount'].sum():,.6f}")
    print(f"veMEZO staked epoch {current_epoch}:       {df_epoch[df_epoch['token'] == 'veMEZO']['amount'].sum():,.6f}")
    print(f"Total stakes epoch {current_epoch}:       {df_epoch['wallet_address'].count():,}")
    print(f"Total stakers epoch {current_epoch}:      {df_epoch['wallet_address'].nunique():,}")
    print(f"Permanent locks epoch {current_epoch}:    {df_epoch[df_epoch['is_permanent'] == True]['wallet_address'].count():,}")
    print(f"Total veBTC earned epoch {current_epoch}: {df_epoch[df_epoch['token'] == 'veBTC']['total_earned'].sum():,.6f}")
    print(f"Total veMEZO earned epoch {current_epoch}: {df_epoch[df_epoch['token'] == 'veMEZO']['total_earned'].sum():,.6f}")
    
    print(f"{'─' * 60}\n")

    # last complete epoch stats
    if current_epoch > 0:
        last_complete_epoch = current_epoch - 1
        
        # Calculate last complete epoch start and end dates
        last_epoch_start = epoch_0_start + timedelta(days=last_complete_epoch * 7)
        last_epoch_end = last_epoch_start + timedelta(days=6)  # End of epoch (Wednesday)
        
        # Filter data for last complete epoch
        df_last_epoch = df[
            (df["date_staked"].astype(str) >= last_epoch_start.strftime("%Y-%m-%d")) &
            (df["date_staked"].astype(str) <= last_epoch_end.strftime("%Y-%m-%d"))
        ]
        
        print(f"\n{'─' * 60}")
        print(f"LAST COMPLETE EPOCH {last_complete_epoch} STAKE & VOTE SUMMARY ({last_epoch_start} to {last_epoch_end}) \n")
        
        print(f"veBTC staked epoch {last_complete_epoch}:        {df_last_epoch[df_last_epoch['token'] == 'veBTC']['amount'].sum():,.6f}")
        print(f"veMEZO staked epoch {last_complete_epoch}:       {df_last_epoch[df_last_epoch['token'] == 'veMEZO']['amount'].sum():,.6f}")
        print(f"Total stakes epoch {last_complete_epoch}:       {df_last_epoch['wallet_address'].count():,}")
        print(f"Total stakers epoch {last_complete_epoch}:      {df_last_epoch['wallet_address'].nunique():,}")
        print(f"Permanent locks epoch {last_complete_epoch}:    {df_last_epoch[df_last_epoch['is_permanent'] == True]['wallet_address'].count():,}")
        print(f"Total veBTC earned epoch {last_complete_epoch}: {df_last_epoch[df_last_epoch['token'] == 'veBTC']['total_earned'].sum():,.6f}")
        print(f"Total veMEZO earned epoch {last_complete_epoch}: {df_last_epoch[df_last_epoch['token'] == 'veMEZO']['total_earned'].sum():,.6f}")
        
        print(f"{'─' * 60}\n")

    # 7-day stats
    df_7d = df[df["date_staked"].astype(str) >= (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")]

    print(f"\n{'─' * 60}")
    print("7-DAY STAKE & VOTE SUMMARY \n")

    print(f"veBTC staked 7d:              {df_7d[df_7d['token'] == 'veBTC']['amount'].sum():,.6f}")
    print(f"veMEZO staked 7d:             {df_7d[df_7d['token'] == 'veMEZO']['amount'].sum():,.6f}")
    print(f"Total stakes 7d:              {df_7d['wallet_address'].count():,}")
    print(f"Total stakers 7d:             {df_7d['wallet_address'].nunique():,}")
    print(f"Permanent locks 7d:           {df_7d[df_7d['is_permanent'] == True]['wallet_address'].count():,}")
    print(f"Total veBTC earned 7d:        {df_7d[df_7d['token'] == 'veBTC']['total_earned'].sum():,.6f}")
    print(f"Total veMEZO earned 7d:       {df_7d[df_7d['token'] == 'veMEZO']['total_earned'].sum():,.6f}")
    
    print(f"{'─' * 60}\n")

    # total stats

    df_public = df[df["date_staked"] >= '2025-12-18'] # launch date
    print(df_public['is_permanent'].value_counts())

    print(f"\n{'─' * 60}")
    print("ALL-TIME STAKE & VOTE SUMMARY \n")

    print(f"Total veBTC staked:            {df[df["token"] == "veBTC"]["amount"].sum():,.6f}")
    print(f"Total veMEZO staked:           {df[df["token"] == "veMEZO"]["amount"].sum():,.6f}")
    print(f"Total stakes:                  {df_public["wallet_address"].count():,}")
    print(f"Total stakers:                 {df_public["wallet_address"].nunique():,}")
    print(f"Total permanent locks:         {df_public[df_public["is_permanent"] == True]["wallet_address"].count():,}")
    print(f"Total veBTC earned:            {df[df["token"] == "veBTC"]["total_earned"].sum():,.6f}")
    print(f"Total veMEZO earned:           {df[df["token"] == "veMEZO"]["total_earned"].sum():,.6f}")
    
    print(f"{'─' * 60}\n")

# ==================================================
# MAIN PROCESSING PIPELINE
# ==================================================

def main(test_mode=False, skip_bigquery=False):

    ProgressIndicators.print_header("📌 STAKE & VOTE DATA PROCESSING PIPELINE")

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
            raw_ve_locks = fetch_voting_escrow_data(
                name=SubgraphClient.VOTING_ESCROW_MAINNET_SUBGRAPH, 
                query=VotingEscrowQueries.GET_VOTE_STAKES, 
                query_key= 'stakes'
            )

            save_to_csv(raw_ve_locks, 'raw_ve_locks')
        
        else:
            raw_ve_locks = pd.read_csv('raw_ve_locks.csv')

        if not skip_bigquery:
            raw_datasets = [
                (raw_ve_locks, 'raw_data', 'votes_ve_locks_raw', 'id')
            ]

            for df, database, table_name, id_column in raw_datasets:
                upload_to_bigquery(bq, df, database, table_name, id_column)

        # ==================================================
        # CLEAN VOTING ESCROW DATA
        # ==================================================

        stg_ve_locks = clean_voting_escrow_data(raw_ve_locks)

        if not test_mode:
            save_to_csv(stg_ve_locks, 'stg_ve_locks')

        if not skip_bigquery:
            stg_datasets = [
                (stg_ve_locks, 'staging', 'votes_ve_locks_stg', 'txn_id')
            ]
            for df, database, table_name, id_column in stg_datasets:
                upload_to_bigquery(bq, df, database, table_name, id_column)

        ProgressIndicators.print_header("🚀 PROCESSING COMPLETED SUCCESSFULLY 🚀")

        print_summary_stake_and_vote_statistics(stg_ve_locks)

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