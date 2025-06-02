import pandas as pd
from datetime import datetime

from mezo.currency_utils import replace_token_labels, format_currency_columns, get_token_prices
from mezo.currency_config import TOKEN_MAP, TOKEN_TYPE_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import load_raw_data
from scripts.get_raw_data import get_all_bridge_transactions
from mezo.clients import SupabaseClient

supabase = SupabaseClient()

# get raw data from subgraph
get_all_bridge_transactions()

updated_on = datetime.today().date()
raw_bridge_txns = load_raw_data(f'{updated_on}_bridge_txns.csv')
bridges = raw_bridge_txns.copy()

# gets USD price data for each token from Coingecko API
tokens = get_token_prices()
token_usd_prices = tokens.T.reset_index() # transpose the df

df_formatted = replace_token_labels(bridges, TOKEN_MAP) # convert token addresses to tickers
df_formatted2 = format_currency_columns(df_formatted, ['amount'], 'token') # make token amts human readable
df_formatted3 = format_datetimes(df_formatted2, ['timestamp_']) # converts UNIX dt to human readable date

bridges_formatted = df_formatted3.copy()

# create a dataframe grouped by bridged tokens
bridge_by_token = bridges_formatted.groupby('token').agg(
    amount = ('amount', 'sum'),
    transactions = ('timestamp_', 'count')
).reset_index()

# compute token price data in USD
bridge_by_token['type'] = bridge_by_token['token'].map(TOKEN_TYPE_MAP) # assign a "type" to each token
bridge_by_token['index'] = bridge_by_token['token'].map(TOKENS_ID_MAP) # maps Coingecko token ID to the token
bridge_by_token_usd = pd.merge(bridge_by_token, token_usd_prices, how='left', on='index') # merges so USD price maps to token
bridge_by_token_usd['amount_usd'] = bridge_by_token_usd['amount'] * bridge_by_token_usd['usd'] # computes the total bridged amount for a token in USD

final_bridge_by_token = bridge_by_token_usd.copy()

# compute summary data points
total_amt_bridged = final_bridge_by_token['amount_usd'].sum()
total_bridge_txns = final_bridge_by_token['transactions'].sum()
total_wallets = pd.DataFrame(bridges_formatted['depositor'].unique()).count()[0]
total_btc_bridged = final_bridge_by_token.loc[final_bridge_by_token['type'] == 'bitcoin']['amount'].sum()
total_stablecoins_bridged = final_bridge_by_token.loc[final_bridge_by_token['type'] == 'stablecoin']['amount'].sum()
total_T_bridged = final_bridge_by_token.loc[final_bridge_by_token['type'] == 'ethereum']['amount'].sum()

d = {
    'total_amt_bridged' : [total_amt_bridged], 
    'total_transactions' : [total_bridge_txns], 
    'total_wallets' : [total_wallets],
    'total_btc_bridged' : [total_btc_bridged], 
    'total_stablecoins_bridged' : [total_stablecoins_bridged], 
    'total_T_bridged' : [total_T_bridged]
}

bridge_summary = pd.DataFrame(data=d)
bridge_by_token_df = final_bridge_by_token[['token', 'amount', 'transactions', 'amount_usd']]
bridge_by_token_df.dtypes

supabase.update_supabase('mainnet_bridge_by_token', bridge_by_token_df)
supabase.update_supabase('mainnet_bridge_summary', bridge_summary)