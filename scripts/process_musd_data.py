import pandas as pd
from dotenv import load_dotenv
import os
import requests
from mezo.currency_utils import format_musd_currency_columns, get_token_price
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pct_change_columns, add_cumulative_columns
from mezo.clients import SupabaseClient, BigQueryClient
from scripts.get_raw_data import get_all_loans, get_liquidation_data, get_trove_liquidated_data

load_dotenv(dotenv_path='../.env', override=True)
COINGECKO_KEY = os.getenv('COINGECKO_KEY')

supabase = SupabaseClient()
bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

# import raw data
raw_loans = get_all_loans()
raw_liquidations = get_liquidation_data()
raw_troves_liquidated = get_trove_liquidated_data()

# Upload raw data to BigQuery
print("📤 Uploading raw data to BigQuery...")
if raw_loans is not None and len(raw_loans) > 0:
    raw_loans['id'] = range(1, len(raw_loans) + 1)
    bq.update_table(raw_loans, 'raw_data', 'musd_loans_raw')
    print("✅ Uploaded raw_loans to BigQuery")

if raw_liquidations is not None and len(raw_liquidations) > 0:
    raw_liquidations['id'] = range(1, len(raw_liquidations) + 1)
    bq.update_table(raw_liquidations, 'raw_data', 'musd_liquidations_raw')
    print("✅ Uploaded raw_liquidations to BigQuery")

if raw_troves_liquidated is not None and len(raw_troves_liquidated) > 0:
    raw_troves_liquidated['id'] = range(1, len(raw_troves_liquidated) + 1)
    bq.update_table(raw_troves_liquidated, 'raw_data', 'musd_troves_liquidated_raw')
    print("✅ Uploaded raw_troves_liquidated to BigQuery")

# helpers
def clean_loan_data(raw, sort_col, date_cols, currency_cols):
    df = raw.copy().sort_values(by=sort_col, ascending=False)
    df = format_datetimes(df, date_cols)
    df = format_musd_currency_columns(df, currency_cols)
    df['count'] = 1
    df['id'] = range(1, len(df) + 1)

    return df

def find_coll_ratio(df, token_id):
    """Computes the collateralization ratio"""
    usd = get_token_price(token_id)
    df['coll_usd'] = df['coll'] * usd
    df['coll_ratio'] = (df['coll_usd']/df['principal'] ).fillna(0)

    return df

def get_loans_subset(df, operation: int, equals):
    """Create a df with only new, adjusted, or closed loans
    0 = opened, 1 = closed, 2 = adjusted
    note: operation = 2 also includes liquidated loans, so we have to remove those manually
    """
    df['operation'] = df['operation'].astype(int)
    if equals is True:
        adjusted = df.loc[df['operation'] == operation]
    elif equals is False:
        adjusted = df.loc[df['operation'] != operation]

    return adjusted

def process_liquidation_data(liquidations, troves_liquidated):
    # Merge raw liquidation data from two queries
    liquidation_df_merged = pd.merge(
        liquidations, 
        troves_liquidated, 
        how='left', 
        on='transactionHash_'
    )

    liquidation_df_merged = liquidation_df_merged[
        ['timestamp__x', 
        'liquidatedPrincipal', 
        'liquidatedInterest', 
        'liquidatedColl', 
        'borrower',
        'transactionHash_',
        'count_x'
        ]
    ]

    liquidations_df_final = liquidation_df_merged.rename(
        columns = {
            'timestamp__x': 'timestamp_', 
            'liquidatedPrincipal': 'principal', 
            'liquidatedInterest': 'interest',
            'liquidatedColl': 'coll',
            'count_x': 'count'
        }
    )

    liquidations_final = liquidations_df_final.copy()
    liquidations_final['coll'] = liquidations_final['coll'].astype(float)

    return liquidations_final

#####################################################

# clean raw data
loans = clean_loan_data(
    raw_loans, 
    sort_col='timestamp_', 
    date_cols=['timestamp_'], 
    currency_cols=['principal', 'coll', 'stake', 'interest']
)

loans = find_coll_ratio(loans, 'bitcoin')

liquidations = clean_loan_data(
    raw_liquidations,
    sort_col='timestamp_',
    date_cols=['timestamp_'],
    currency_cols=['liquidatedPrincipal', 'liquidatedInterest', 'liquidatedColl']
)

troves_liquidated = clean_loan_data(
    raw_troves_liquidated,
    sort_col='timestamp_',
    date_cols=['timestamp_'],
    currency_cols=['debt', 'coll']
)

# Create df for liquidated loans
liquidations_final = process_liquidation_data(liquidations, troves_liquidated)

# Create df's for new loans, closed loans, and adjusted loans and upload to BigQuery
new_loans = get_loans_subset(loans, 0, True)
closed_loans = get_loans_subset(loans, 1, True)
adjusted_loans = get_loans_subset(loans, 2, True) # Only adjusted loans (incl multiple adjustments from a single user)

## Remove liquidations from adjusted loans
liquidated_borrowers = liquidations_final['borrower'].unique()
adjusted_loans = adjusted_loans[~adjusted_loans['borrower'].isin(liquidated_borrowers)]

##################################

# Get latest loans
latest_loans = loans.drop_duplicates(subset='borrower', keep='first')

# Create df with only open loans
latest_open_loans = get_loans_subset(latest_loans, 1, False)

# Remove liquidated loans from list of latest loans w/o closed loans
latest_open_loans = latest_open_loans[~latest_open_loans['borrower'].isin(liquidated_borrowers)]

##################################

# Break down adjusted loan types for analysis
adjusted_loans = adjusted_loans.sort_values(by=['borrower', 'timestamp_'])
first_tx = adjusted_loans.groupby('borrower').first().reset_index()

adjusted_loans_merged = adjusted_loans.merge(
    first_tx[['borrower', 'principal', 'coll']], 
    on='borrower', 
    suffixes=('', '_initial')
)

## Loan increases
increased_loans = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                        > adjusted_loans_merged['principal_initial']].copy()
increased_loans['type'] = 1

## Collateral changes
coll_increased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                       > adjusted_loans_merged['coll_initial']].copy()
coll_increased['type'] = 2

coll_decreased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                       < adjusted_loans_merged['coll_initial']].copy()
coll_decreased['type'] = 3

## MUSD Repayments
principal_decreased = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                            < adjusted_loans_merged['principal_initial']].copy()
principal_decreased['type'] = 4

## Create final_adjusted_loans dataframe with type column
final_adjusted_loans = pd.concat([
    increased_loans,
    coll_increased, 
    coll_decreased,
    principal_decreased
], ignore_index=True)

# Upload new_loans, closed_loans, and adjusted_loans to BigQuery
print("📤 Uploading loan subset data to BigQuery...")
if new_loans is not None and len(new_loans) > 0:
    bq.update_table(new_loans, 'staging', 'new_loans_clean')
    print("✅ Uploaded new_loans to BigQuery")

if closed_loans is not None and len(closed_loans) > 0:
    bq.update_table(closed_loans, 'staging', 'closed_loans_clean')
    print("✅ Uploaded closed_loans to BigQuery")

if latest_open_loans is not None and len(latest_open_loans) > 0:
    bq.update_table(latest_open_loans, 'staging', 'open_loans_clean')
    print("✅ Uploaded latest_open_loans to BigQuery")

if final_adjusted_loans is not None and len(final_adjusted_loans) > 0:
    final_adjusted_loans['id'] = range(1, len(final_adjusted_loans) + 1)
    bq.update_table(final_adjusted_loans, 'staging', 'adjusted_loans_clean')
    print("✅ Uploaded final_adjusted_loans to BigQuery")

# Create daily dataframe
daily_new_loans = new_loans.groupby(['timestamp_']).agg(
    loans_opened = ('count', 'sum'),
    borrowers = ('borrower', lambda x: x.nunique()),
    principal = ('principal', 'sum'),
    collateral = ('coll', 'sum'),
    interest = ('interest', 'sum')
).reset_index()

daily_closed_loans = closed_loans.groupby(['timestamp_']).agg(
    loans_closed = ('count', 'sum'),
    borrowers_who_closed = ('borrower', lambda x: x.nunique())
).reset_index()

daily_new_and_closed_loans = pd.merge(daily_new_loans, daily_closed_loans, how = 'outer', on = 'timestamp_').fillna(0)
daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']] = daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']].astype('int')      
daily_adjusted_loans = adjusted_loans.groupby(['timestamp_']).agg(
    loans_adjusted = ('count', 'sum'),
    borrowers_who_adjusted = ('borrower', lambda x: x.nunique())
).reset_index()

daily_loan_data = pd.merge(daily_new_and_closed_loans, daily_adjusted_loans, how='outer', on='timestamp_').fillna(0)
daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']] = daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']].astype(int)

daily_balances = latest_loans.groupby(['timestamp_']).agg(
    musd = ('principal', 'sum'),
    interest = ('interest', 'sum'),
    collateral = ('coll', 'sum')
).reset_index()

daily_balances = daily_balances.rename(
    columns={'musd': 'net_musd', 
             'interest': 'net_interest',
             'collateral': 'net_coll'}
)

daily_loans_merged = pd.merge(daily_loan_data, daily_balances, how='outer', on='timestamp_')

cols = {
    'timestamp_': 'date', 
    'principal': 'gross_musd', 
    'collateral': 'gross_coll', 
    'interest': 'gross_interest',
    'borrowers_who_closed': 'closers', 
    'borrowers_who_adjusted': 'adjusters'
}

daily_loans_merged = daily_loans_merged.rename(columns = cols)

daily_musd_final = add_rolling_values(daily_loans_merged, 30, ['net_musd', 'net_interest', 'net_coll']).fillna(0)
daily_musd_final_2 = add_cumulative_columns(daily_musd_final, ['net_musd', 'net_interest', 'net_coll'])
daily_musd_final_3 = add_pct_change_columns(daily_musd_final_2, ['net_musd', 'net_interest', 'net_coll'], 'daily').fillna(0)
final_daily_musd = daily_musd_final_3.replace([float('inf'), -float('inf')], 0)
final_daily_musd['date'] = pd.to_datetime(final_daily_musd['date']).dt.strftime('%Y-%m-%d')

# Upload daily loan data to BigQuery
print("📤 Uploading daily loans data to BigQuery...")
if final_daily_musd is not None and len(final_daily_musd) > 0:
    final_daily_musd['id'] = range(1, len(final_daily_musd) + 1)
    bq.update_table(final_daily_musd, 'staging', 'daily_loans_clean')
    print("✅ Uploaded final_daily_musd to BigQuery")

# Convert integer columns that may have become floats back to integers
integer_columns = ['loans_opened', 'borrowers', 'loans_closed', 'closers', 'loans_adjusted', 'adjusters']
for col in integer_columns:
    if col in final_daily_musd.columns:
        final_daily_musd[col] = final_daily_musd[col].astype(int)

supabase.update_supabase('mainnet_musd_daily', final_daily_musd)

##################################

# Create summary dataframes
all_time_musd_borrowed = new_loans['principal'].sum() # the historical amount of MUSD taken out in loans
all_time_musd_loans = new_loans['count'].sum() # the historical number of MUSD loans opened.
all_time_musd_borrowers = new_loans['borrower'].nunique() # number of unique borrowers who have ever opened a loan
all_time_closed_loans = closed_loans['count'].sum()
all_time_adjustments = adjusted_loans['count'].sum()

increase_txns = increased_loans['count'].sum()
loans_increased = increased_loans['borrower'].nunique()

increase_coll_txns = coll_increased['count'].sum()
decrease_coll_txns = coll_decreased['count'].sum()
loans_with_coll_increased = coll_increased['borrower'].nunique()
loans_with_coll_decreased = coll_decreased['borrower'].nunique()

partial_repayment_txns = principal_decreased['count'].sum()
loans_with_partial_repayments = principal_decreased['borrower'].nunique()

open_loans = latest_open_loans['count'].sum()

liquidated_loans = liquidations_final['count'].sum()
interest_liquidated = liquidations_final['interest'].sum()
coll_liquidated = liquidations_final['coll'].sum()

# system health
TCR = latest_open_loans['coll_ratio'].mean()*100
system_coll = latest_open_loans['coll'].sum()
system_debt = latest_open_loans['principal'].sum() + latest_open_loans['interest'].sum()

d = {
    'all_time_musd_borrowed' : all_time_musd_borrowed, 
    'all_time_loans' : all_time_musd_loans,
    'all_time_borrowers' : all_time_musd_borrowers, 
    'system_debt' : system_debt, 
    'system_coll' : system_coll,
    'TCR' : TCR, 
    'open_loans' : open_loans,
    'closed_loans' : all_time_closed_loans,
    'liquidated_loans' : liquidated_loans,
    'interest_liquidated' : interest_liquidated,
    'collateral_liquidated' : coll_liquidated
}

musd_summary = pd.DataFrame([d])
supabase.update_supabase('mainnet_musd_borrow_summary', musd_summary)

## DF with adjustments summary
e = {
    'total_adjustments' : all_time_adjustments, 
    'total_principal_increase' : increase_txns, 
    'unique_principal_increase' : loans_increased,
    'total_coll_increase' : increase_coll_txns,
    'unique_coll_increase' : loans_with_coll_increased, 
    'total_coll_decreasee' : decrease_coll_txns, 
    'unique_coll_decrease' : loans_with_coll_decreased, 
    'total_partial_repayments' : partial_repayment_txns, 
    'unique_partial_repayments' : loans_with_partial_repayments
}

musd_adjustments_summary = pd.DataFrame([e])
supabase.update_supabase('mainnet_musd_adjustments_summary', musd_adjustments_summary)

## Newly opened loan averages
avg_new_loan_amt = new_loans['principal'].mean()
median_new_loan_amt = new_loans['principal'].median()

avg_new_loan_coll = new_loans['coll'].mean()
median_new_loan_coll = new_loans['coll'].median()

avg_new_loan_coll_ratio = new_loans['coll_ratio'].mean()
median_new_loan_coll_ratio = new_loans['coll_ratio'].median()

n = {
    'avg_new_loan' : avg_new_loan_amt, 
    'avg_new_loan_coll' : avg_new_loan_coll, 
    'median_new_loan' : median_new_loan_amt,
    'median_new_loan_coll' : median_new_loan_coll,
    'avg_new_loan_coll_ratio' : avg_new_loan_coll_ratio,
    'median_new_loan_coll_ratio' : median_new_loan_coll_ratio
}

new_loan_averages_summary = pd.DataFrame([n])
supabase.update_supabase('mainnet_musd_new_loan_averages', new_loan_averages_summary)

## Current loan averages
avg_loan_amt = latest_open_loans['principal'].mean()
avg_coll = latest_open_loans['coll'].mean()
avg_interest = latest_open_loans['interest'].mean()
avg_coll_ratio = latest_open_loans['coll_ratio'].mean()

median_loan_amt = latest_open_loans['principal'].median()
median_coll = latest_open_loans['coll'].median()
median_interest = latest_open_loans['interest'].median()
median_coll_ratio = latest_open_loans['coll_ratio'].median()

avg_loans_per_user = all_time_musd_loans/all_time_musd_borrowers

a = {
    'avg_loan_amount' : avg_loan_amt, 
    'avg_collateral' : avg_coll, 
    'avg_interest' : avg_interest,
    'avg_collateralization_ratio' : avg_coll_ratio,
    'avg_loans_per_user' : avg_loans_per_user,
    'median_loan_amount' : median_loan_amt,
    'median_collateral' : median_coll,
    'median_interest' : median_interest,
    'median_collateralization_ratio' : median_coll_ratio
}

musd_averages_summary = pd.DataFrame([a])
supabase.update_supabase('mainnet_musd_averages', musd_averages_summary)

# System health
TCR = latest_open_loans['coll_ratio'].mean()*100
system_coll = latest_open_loans['coll'].sum()
system_debt = latest_open_loans['principal'].sum() + latest_open_loans['interest'].sum()

h = {
    'TCR': TCR,
    'system_collateral': system_coll,
    'system_debt': system_debt
}

musd_system_health = pd.DataFrame([h])
supabase.append_to_supabase('mainnet_musd_system_health', musd_system_health)

# raw musd token transfers data
def fetch_data(endpoint: str) -> pd.DataFrame:
        """Fetch data from the specified API endpoint."""

        base_url = 'http://api.explorer.mezo.org/api/v2/'
        url = f"{base_url}/{endpoint}"
        all_data = []
        next_page_params = None
        timeout = 10
        
        while True:
            response = requests.get(url, params=next_page_params or {}, timeout=timeout)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data: {response.status_code}")

            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            all_data.append(pd.json_normalize(items))
            next_page_params = data.get("next_page_params")
            if not next_page_params:
                break

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

musd_token_address = '0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186'
musd_transfers = fetch_data(f'tokens/{musd_token_address}/transfers')

print(musd_transfers)
print(musd_transfers.columns)

musd_transfers_short = musd_transfers[[
    'timestamp','from.hash', 'to.hash', 'type', 'method', 
    'total.value', 'tx_hash', 'block_number', 
    'from.is_contract', 'from.is_scam', 
    'to.is_contract', 'to.is_scam'
    ]]

musd_transfers_short['timestamp'] = pd.to_datetime(musd_transfers_short['timestamp']).dt.date
format_musd_currency_columns(musd_transfers_short, ['total.value'])
musd_transactions = musd_transfers_short.copy()

print(musd_transactions.loc[musd_transactions['from.hash'] == '0x0000000000000000000000000000000000000000'].nunique())

# get holder data
base_url = 'http://api.explorer.mezo.org/api/v2/tokens/'
musd_token_address = '0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186'

url = f'{base_url}{musd_token_address}/counters'

response = requests.get(url, timeout=10)
data = response.json()
dat = pd.json_normalize(data)
musd_holders = pd.DataFrame(dat)

## get token data
url2 = f'{base_url}{musd_token_address}/'

response = requests.get(url2, timeout=10)
data2 = response.json()
dat2 = pd.json_normalize(data2)
musd_token_data = pd.DataFrame(dat2)

print(musd_token_data.columns)

musd_token = pd.merge(musd_token_data, musd_holders, how = 'cross')

musd_token = musd_token[[
    'circulating_market_cap', 'exchange_rate', 
    'holders', 'total_supply', 'volume_24h', 
    'token_holders_count', 'transfers_count']]

format_musd_currency_columns(musd_token, ['total_supply'])
musd_token = musd_token.fillna(0)

supabase.append_to_supabase('mainnet_musd_token_summary', musd_token)
# supabase.update_supabase('mainnet_musd_token_summary', musd_token)

print('🚀 Run successful!')

# # Upload raw MUSD token transfers to BigQuery
# if len(musd_transfers) > 0:
#     bq.update_table(musd_transfers, 'raw_data', 'musd_token_transfers')
#     print("✅ Uploaded raw musd_transfers to BigQuery")