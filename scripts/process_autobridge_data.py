import pandas as pd
from datetime import datetime
from mezo.currency_utils import replace_token_labels, format_currency_columns, get_token_prices
from mezo.currency_config import TOKEN_MAP, TOKEN_TYPE_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import load_raw_data
from scripts.get_raw_data import get_all_autobridge_transactions
from mezo.clients import SupabaseClient

supabase = SupabaseClient()
pd.options.display.float_format = '{:.5f}'.format

# Import raw autobridge data

get_all_autobridge_transactions()

updated_on = datetime.today().date()
raw_autobridge_transactions = load_raw_data(f'{updated_on}_autobridges.csv')

autobridge_df = raw_autobridge_transactions.copy()

# Clean the dataframe with datetime and currency formatting helpers
df_formatted_tokens = replace_token_labels(autobridge_df, TOKEN_MAP)
df_formatted_tokens_and_currency = format_currency_columns(df_formatted_tokens, ['amount'], 'token')
df_formatted_tokens_currency_and_dt = format_datetimes(df_formatted_tokens_and_currency, ['timestamp_'])

# Copy the formatted dataframe to avoid polluting data
df_formatted = df_formatted_tokens_currency_and_dt.copy()
df_formatted['date'] = pd.to_datetime(df_formatted['timestamp_']).dt.date

# Create a dataframe grouped by autobridged tokens
autobridge_by_token_df = df_formatted.groupby('token').agg(
    bridged_amount = ('amount', 'sum'),
    bridged_transactions = ('date', 'count')
).reset_index()

# Add a column that matches the token to its type (bitcoin, stablecoin, ethereum)
autobridge_by_token_df['type'] = autobridge_by_token_df['token'].map(TOKEN_TYPE_MAP)

# Using the Coingecko API, get USD price data for each token autobridged and create a df
tokens = get_token_prices()
token_usd_prices = tokens.T.reset_index() # transpose the df

# Map the Coingecko API token ID to the autobridged token
autobridge_by_token_df['index'] = autobridge_by_token_df['token'].map(TOKENS_ID_MAP)

# Merge on the Coingecko token ID so USD price matches to the autobridged token
autobridge_by_token_df_with_usd = pd.merge(autobridge_by_token_df, token_usd_prices, how='left', on='index')

# Compute the bridged amount in USD
autobridge_by_token_df_with_usd['bridged_amount_usd'] = autobridge_by_token_df_with_usd['bridged_amount'] * autobridge_by_token_df_with_usd['usd']

# Trim down the df to necessary cols only
final_autobridge_df_by_token = autobridge_by_token_df_with_usd[[
    'token', 'bridged_amount', 'bridged_transactions','bridged_amount_usd']]

# Summarize all stats into a dataframe
total_bridged_assets = autobridge_by_token_df_with_usd['bridged_amount_usd'].sum()
total_autobridge_transactions = df_formatted.count()[0]
total_autobridge_depositors = pd.DataFrame(df_formatted['depositor'].unique()).count()[0]
total_bridged_btc_assets = sum(autobridge_by_token_df.loc[autobridge_by_token_df['type'] == 'bitcoin']['bridged_amount'])
total_bridged_stablecoins = sum(autobridge_by_token_df.loc[autobridge_by_token_df['type'] == 'stablecoin']['bridged_amount'])
total_bridged_T = sum(autobridge_by_token_df.loc[autobridge_by_token_df['type'] == 'ethereum']['bridged_amount'])

data = [[total_bridged_assets, total_autobridge_transactions, total_autobridge_depositors,
     total_bridged_btc_assets, total_bridged_stablecoins, total_bridged_T]]

cols = ["total_amt_bridged", "total_transactions", "total_depositors_who_bridged",
        "total_bitcoin_bridged", "total_stablecoins_bridged", "total_T_bridged"]

d = {
    'total_amt_bridged' : total_bridged_assets, 
    'total_transactions' : total_autobridge_transactions, 
    'total_depositors_who_bridged' : total_autobridge_depositors,
    'total_bitcoin_bridged' : total_bridged_btc_assets, 
    'total_stablecoins_bridged' : total_bridged_stablecoins, 
    'total_T_bridged' : total_bridged_T
}


autobridge_summary_df = pd.DataFrame(data=data, columns=cols)

supabase.update_supabase('mainnet_autobridge_summary', autobridge_summary_df)
supabase.update_supabase('mainnet_autobridge_by_token', final_autobridge_df_by_token)

path = f"/Users/laurenjackson/Desktop/mezo-analytics-mainnet/data/processed/"
final_autobridge_df_by_token.to_csv(f'{path}/{updated_on}_autobridges_by_token.csv')
autobridge_summary_df.to_csv(f'{path}/{updated_on}_autobridge_summary.csv')

print("✅ Run successful!")
