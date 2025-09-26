from dotenv import load_dotenv
import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from mezo.currency_utils import format_currency_columns, replace_token_labels
from mezo.currency_config import TOKEN_MAP, TOKEN_TYPE_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import get_token_prices
from mezo.clients import SupabaseClient, BigQueryClient
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
import numpy as np
from datetime import date

date.today()

# ==================================================
# LOAD HELPER FUNCTIONS
# ==================================================

def add_usd_conversions(df, token_column, tokens_id_map, amount_columns=None):
    """
    Add USD price conversions to any token data
    
    Args:
        df: DataFrame containing token data
        token_column: Name of column containing token identifiers
        tokens_id_map: Dictionary mapping tokens to CoinGecko IDs
        amount_columns: List of amount columns to convert, or None for auto-detection
    
    Returns:
        DataFrame with USD conversion columns added
    """
    if token_column not in df.columns:
        raise ValueError(f"Column '{token_column}' not found in DataFrame")
    
    # Get token prices
    prices = get_token_prices()
    if prices is None or prices.empty:
        raise ValueError("No token prices received from API")
    
    token_usd_prices = prices.T.reset_index()
    df_result = df.copy()
    df_result['index'] = df_result[token_column].map(tokens_id_map)
    
    df_with_usd = pd.merge(df_result, token_usd_prices, how='left', on='index')
    
    # Set MUSD price to 1.0 (1:1 with USD)
    df_with_usd.loc[df_with_usd[token_column] == 'MUSD', 'usd'] = 1.0
    
    # Auto-detect amount columns if not provided
    if amount_columns is None:
        amount_columns = [col for col in df.columns if 'amount' in col.lower() and col != 'amount_usd']
    
    # Add USD conversion for each amount column
    for col in amount_columns:
        if col in df_with_usd.columns:
            usd_col_name = f"{col}_usd" if not col.endswith('_usd') else col
            df_with_usd[usd_col_name] = df_with_usd[col] * df_with_usd['usd']
    
    return df_with_usd

def clean_bridge_data(raw, sort_col, date_cols, currency_cols, asset_col):
    """Clean and format bridge transaction data."""
    if not ExceptionHandler.validate_dataframe(raw, "Raw bridge data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col, ascending=False)
    df = replace_token_labels(df, TOKEN_MAP)
    df = format_datetimes(df, date_cols)
    df = format_currency_columns(df, currency_cols, asset_col)
    df['count'] = 1
    return df

# Load environment variables
load_dotenv(dotenv_path='../.env', override=True)
pd.options.display.float_format = '{:.5f}'.format

# ==================================================
# GET RAW BRIDGE DATA
# ==================================================

raw_deposits = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_SUBGRAPH, 
    BridgeQueries.GET_BRIDGE_TRANSACTIONS,
    'assetsLockeds'
)

raw_withdrawals = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH, 
    BridgeQueries.GET_NATIVE_WITHDRAWALS,
    'assetsUnlockeds'
)

# raw_withdrawals = pd.read_csv('test_withdrawals.csv')
# raw_deposits = pd.read_csv('test_deposits.csv')

# ==================================================
# CLEAN DEPOSITS DATA
# ==================================================

# Clean deposits data
deposits = clean_bridge_data(raw_deposits, 'timestamp_', ['timestamp_'], ['amount'], 'token')
deposits_with_usd = add_usd_conversions(
    deposits, 
    token_column='token', 
    tokens_id_map=TOKENS_ID_MAP, 
    amount_columns =['amount']
)
deposits_with_usd['type'] = 'deposit'

deposits_clean = deposits_with_usd.sort_values(by='timestamp_', ascending=True)
deposits_clean = deposits_with_usd[[
    'timestamp_', 'amount', 'token', 'amount_usd',
    'recipient', 'transactionHash_', 'type'
]]

deposits_clean = deposits_clean.rename(columns={'recipient': 'depositor'})

# ==================================================
# CLEAN WITHDRAWALS DATA
# ==================================================

# clean withdrawals data
withdrawals = clean_bridge_data(raw_withdrawals, 'timestamp_', ['timestamp_'], ['amount'], 'token')
withdrawals_with_usd = add_usd_conversions(
    withdrawals, 
    token_column='token', 
    tokens_id_map=TOKENS_ID_MAP, 
    amount_columns=['amount']
)
withdrawals_with_usd['type'] = 'withdrawal'

bridge_map = {'0': 'ethereum', '1': 'bitcoin'}
withdrawals_with_usd['chain'] = withdrawals_with_usd['chain'].map(bridge_map)

withdrawals_clean = withdrawals_with_usd.sort_values(by='timestamp_', ascending=True)
withdrawals_clean = withdrawals_with_usd[[
    'timestamp_', 'amount', 'token', 'amount_usd', 'chain', 
    'recipient', 'sender', 'transactionHash_', 'type'
]]

withdrawals_clean = withdrawals_clean.rename(columns={
    'sender': 'withdrawer', 
    'recipient': 'withdraw_recipient'
    }
)

# ==================================================
# COMBINE DEPOSIT AND WITHDRAWAL DATA
# ==================================================

# Combine data
combined = pd.concat([deposits_clean, withdrawals_clean], ignore_index=True).fillna(0)
combined = combined.sort_values('timestamp_').reset_index(drop=True)


# ==================================================
# GET NET FLOW, TVL, AND VOLUME
# ==================================================

# Calculate net amounts (deposits positive, withdrawals negative)
combined['net_flow'] = np.where(
    combined['type'] == 'deposit',
    combined['amount_usd'],
    -combined['amount_usd']
)

# Calculate absolute amounts for deposits/withdrawals tracking
combined['deposit_amount_usd'] = np.where(
    combined['type'] == 'deposit',
    combined['amount_usd'],
    0
)

combined['withdrawal_amount_usd'] = np.where(
    combined['type'] == 'withdrawal',
    combined['amount_usd'],
    0
)
combined['volume'] = combined['withdrawal_amount_usd'] + combined['deposit_amount_usd']
combined['tvl'] = combined['net_flow'].cumsum()

combined[['timestamp_', 'volume']].sort_values(by='timestamp_', ascending=False)

# ==================================================
# GET NET FLOW AND TVL BY TOKEN
# ==================================================

tvl_df = combined.copy()

daily_tvl_by_token = tvl_df.groupby(['token', 'timestamp_']).agg(
        # TVL metrics (end of day values)
        tvl = ('tvl', 'last'),
        net_flow = ('net_flow', 'sum'),  # Net daily flow
        deposits_usd = ('deposit_amount_usd', 'sum'),  # Total daily deposits
        withdrawals_usd = ('withdrawal_amount_usd', 'sum'),  # Total daily withdrawals
        tx_type = ('type', 'count'),  # Total transactions
    ).reset_index()

deposits = combined[combined['type'] == 'deposit'].groupby(['token', 'timestamp_']).size()
depositors = combined[combined['type'] == 'deposit'].groupby(['token', 'timestamp_'])['depositor'].nunique()
withdrawals = combined[combined['type'] == 'withdrawal'].groupby(['token', 'timestamp_']).size()
withdrawers = combined[combined['type'] == 'withdrawal'].groupby(['token', 'timestamp_'])['withdrawer'].nunique()

daily_tvl = daily_tvl_by_token.set_index(['token', 'timestamp_'])
daily_tvl['deposits'] = deposits
daily_tvl['withdrawals'] = withdrawals
daily_tvl['depositors'] = depositors
daily_tvl['withdrawers'] = withdrawers
daily_tvl['unique_users'] = daily_tvl['withdrawers'] + daily_tvl['depositors']
daily_tvl['total_transactions'] = daily_tvl['deposits'] + daily_tvl['withdrawals']
daily_tvl = daily_tvl.fillna(0).reset_index()

daily_tvl['deposit_withdrawal_ratio'] = np.where(
    daily_tvl['withdrawals_usd'] > 0,
    daily_tvl['deposits_usd'] / daily_tvl['withdrawals_usd'],
    np.inf
)

# TVL - no moving average, but track changes
daily_tvl['tvl_change'] = daily_tvl.groupby('token')['tvl'].diff()
daily_tvl['tvl_change_pct'] = daily_tvl.groupby('token')['tvl'].pct_change()
daily_tvl['tvl_ath'] = daily_tvl.groupby('token')['tvl'].cummax()
daily_tvl['drawdown_from_ath'] = (
    (daily_tvl['tvl'] - daily_tvl['tvl_ath']) / 
    daily_tvl['tvl_ath']
)
for col in ['deposits', 'withdrawals', 'net_flow']:
    daily_tvl[f'{col}_ma7'] = daily_tvl.groupby('token')[col].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    daily_tvl[f'{col}_ma30'] = daily_tvl.groupby('token')[col].transform(lambda x: x.rolling(window=30, min_periods=1).mean())

# ==================================================
# CALCULATE TVL, NET FLOW OVERALL
# ==================================================

# Create a combined df again, but only sort by timestamp, not token/timestamp
combined_all = pd.concat([deposits_clean, withdrawals_clean], ignore_index=True)
combined_all = combined_all.sort_values(['timestamp_']).reset_index(drop=True)
combined_all = combined_all.fillna(0)

# Calculate net amounts (deposits positive, withdrawals negative)
combined_all['net_flow'] = np.where(
    combined_all['type'] == 'deposit',
    combined_all['amount_usd'],
    -combined_all['amount_usd']
)

# Calculate absolute amounts for deposits/withdrawals tracking
combined_all['deposit_amount_usd'] = np.where(
    combined_all['type'] == 'deposit',
    combined_all['amount_usd'],
    0
)

combined_all['withdrawal_amount_usd'] = np.where(
    combined_all['type'] == 'withdrawal',
    combined_all['amount_usd'],
    0
)
combined_all['tvl'] = combined_all['net_flow'].cumsum()
combined['volume'] = combined['withdrawal_amount_usd'] + combined['deposit_amount_usd']


tvl_df_all = combined_all.copy()

daily_tvl_all = tvl_df_all.groupby(['timestamp_']).agg(
        # TVL metrics (end of day values)
        tvl = ('tvl', 'last'),
        net_flow = ('net_flow', 'sum'),  # Net daily flow
        deposits_usd = ('deposit_amount_usd', 'sum'),  # Total daily deposits
        withdrawals_usd = ('withdrawal_amount_usd', 'sum'),  # Total daily withdrawals
        tx_type = ('type', 'count'),  # Total transactions
    ).reset_index()

# Calculate deposit and withdrawal counts
deposit_counts = combined_all[combined_all['type'] == 'deposit'].groupby(['timestamp_']).size()
depositors = combined_all[combined_all['type'] == 'deposit'].groupby(['timestamp_'])['depositor'].nunique()
withdrawal_counts = combined_all[combined_all['type'] == 'withdrawal'].groupby(['timestamp_']).size()
withdrawers = combined_all[combined_all['type'] == 'withdrawal'].groupby(['timestamp_'])['withdrawer'].nunique()
    
# Add transaction counts to daily metrics
daily_tvl_all = daily_tvl_all.set_index(['timestamp_'])
daily_tvl_all['deposits'] = deposit_counts
daily_tvl_all['unique_depositors'] = depositors
daily_tvl_all['withdrawals'] = withdrawal_counts
daily_tvl_all['unique_withdrawers'] = withdrawers
daily_tvl_all['unique_users'] = daily_tvl_all['unique_withdrawers'] + daily_tvl_all['unique_depositors']
daily_tvl_all['total_transactions'] = daily_tvl_all['deposits'] + daily_tvl_all['withdrawals']
daily_tvl_all = daily_tvl_all.fillna(0).reset_index()

# Calculate protocol-wide additional metrics
daily_tvl_all['deposit_withdrawal_ratio'] = np.where(
    daily_tvl_all['withdrawals'] > 0,
    daily_tvl_all['deposits'] / daily_tvl_all['withdrawals'],
    np.inf
)
    
# TVL - no moving average, but track changes
daily_tvl_all['tvl_change'] = daily_tvl_all['tvl'].diff()
daily_tvl_all['tvl_change_pct'] = daily_tvl_all['tvl'].pct_change()
daily_tvl_all['tvl_ath'] = daily_tvl_all['tvl'].cummax()
daily_tvl_all['drawdown_from_ath'] = (
    (daily_tvl_all['tvl'] - daily_tvl_all['tvl_ath']) / 
    daily_tvl_all['tvl_ath']
)
for col in ['deposits', 'withdrawals', 'net_flow']:
    daily_tvl_all[f'{col}_ma7'] = daily_tvl[col].rolling(window=7).mean()
    daily_tvl_all[f'{col}_ma30'] = daily_tvl[col].rolling(window=7).mean()

daily_tvl_all = daily_tvl_all.fillna(0)

# stats

daily_tvl_all.columns

tvl_total = daily_tvl_all['tvl'].iloc[-1]
tvl_total_change = daily_tvl_all['tvl_change_pct'].iloc[-1]

# ==================================================
# CALCULATE VOLUME BY TOKEN
# ==================================================


vol_by_token = combined.copy()
# vol_by_token = vol_by_token[[
#     'timestamp_', 'amount', 'token', 'amount_usd', 
#     'type', 'net_flow', 'deposit_amount_usd', 'withdrawal_amount_usd', 
#     'chain', 'depositor', 'withdrawer', 'withdraw_recipient', 
#     'transactionHash_'
# ]] # remove tvl and volume to recalculate by token

vol_by_token = vol_by_token.sort_values(['token', 'timestamp_']).reset_index(drop=True)
vol_by_token['token_volume'] = vol_by_token.groupby('token')['volume'].diff().fillna(0)
vol_by_token.columns

daily_vol_by_token = vol_by_token.groupby(['token', 'timestamp_']).agg(
    amount = ('amount', 'sum'),
    amount_usd = ('amount_usd', 'sum'),
    unique_withdrawers = ('withdrawer', 'nunique'),
    unique_depositors = ('depositor', 'nunique'),
    net_flow = ('net_flow', 'sum'),
    inflow = ('deposit_amount_usd', 'sum'),
    outflow = ('withdrawal_amount_usd', 'sum')
).reset_index()

deposits = vol_by_token[vol_by_token['type'] == 'deposit'].groupby(['token', 'timestamp_']).size()
depositors = vol_by_token[vol_by_token['type'] == 'deposit'].groupby(['token', 'timestamp_'])['depositor'].nunique()
withdrawals = vol_by_token[vol_by_token['type'] == 'withdrawal'].groupby(['token', 'timestamp_']).size()
withdrawers = vol_by_token[vol_by_token['type'] == 'withdrawal'].groupby(['token', 'timestamp_'])['withdrawer'].nunique()

daily_vol_by_token = daily_vol_by_token.set_index(['token', 'timestamp_'])
daily_vol_by_token['deposits'] = deposits
daily_vol_by_token['withdrawals'] = withdrawals
daily_vol_by_token['depositors'] = depositors
daily_vol_by_token['withdrawers'] = withdrawers
daily_vol_by_token['unique_users'] = daily_vol_by_token['withdrawers'] + daily_vol_by_token['depositors']
daily_vol_by_token['total_transactions'] = daily_vol_by_token['deposits'] + daily_vol_by_token['withdrawals']
daily_vol_by_token['token_volume'] = daily_vol_by_token['inflow'] + daily_vol_by_token['outflow']
daily_vol_by_token['tvl'] = daily_vol_by_token['net_flow'].cumsum()
daily_vol_by_token = daily_vol_by_token.reset_index()
daily_vol_by_token = daily_vol_by_token.fillna(0)

# separate it into 

daily_vol_by_token_1 = daily_vol_by_token[[
    'token', 'timestamp_', 'net_flow', 'inflow', 'outflow', 'token_volume'
]]

daily_vol_by_token_2 = daily_vol_by_token[[
    'token', 'timestamp_', 'deposits', 'withdrawals', 'depositors', 'withdrawers'
]]

dfs = [daily_vol_by_token_1, daily_vol_by_token_2]
pivoted_dfs = []

for df in dfs:
    # Pivot data by token
    pivoted_df = df.pivot(
        index='timestamp_', columns='token'
    ).fillna(0)

    # Flatten column names
    pivoted_df.columns = [
    '_'.join(col).strip() for col in pivoted_df.columns.values]

    pivoted_df = pivoted_df.reset_index()

    pivoted_dfs.append(pivoted_df)

dvbt1_final = pivoted_dfs.pop(0)
dvbt2_final = pivoted_dfs.pop(0)




# Make a copy to avoid modifying original
df = bridge_volume.copy()

# Ensure timestamp is datetime and create date column
df['timestamp_'] = pd.to_datetime(df['timestamp_'])
df['timestamp_'] = df['timestamp_'].dt.date

# Calculate daily volume (difference from previous day for each token)
df['daily_token_volume'] = df.groupby('token')['volume'].diff().fillna(0)

# For first entry of each token, use total as daily
first_entries = df.groupby('token').first().index
mask = df.set_index('token').index.isin(first_entries)
df.loc[mask & df['volume'].isna(), 'volume'] = \
    df.loc[mask & df['volume'].isna(), 'volume']

daily_bridge_volume = df.groupby(['timestamp_', 'token']).agg({
        'daily_token_volume': 'last',  # Daily volume for the token
    }).reset_index()

# Calculate 7-day moving average for each pool
daily_bridge_volume['volume_7d_ma'] = daily_bridge_volume.groupby('token')['daily_token_volume'].transform(
    lambda x: x.rolling(window=7, min_periods=1).mean()
)

# Calculate growth rate (day-over-day percentage change)
daily_bridge_volume['growth_rate'] = daily_bridge_volume.groupby('token')['daily_token_volume'].transform(
    lambda x: x.pct_change() * 100
)

# Identify significant volume days (> 90th percentile for each pool)
daily_bridge_volume['is_significant_volume'] = daily_bridge_volume.groupby('token')['daily_token_volume'].transform(
    lambda x: x > x.quantile(0.9)
)

# Add token rank by daily volume
daily_bridge_volume['daily_rank'] = daily_bridge_volume.groupby('timestamp_')['daily_token_volume'].rank(
    method='dense', ascending=False
)

# ========================================
# AGGREGATE DAILY VOLUME STATS (ALL TOKENS)
# ========================================

daily_bridge_vol_all = daily_bridge_volume.groupby('timestamp_').agg({
    'daily_token_volume': 'sum',      # Total volume across all pools
    'is_significant_volume': 'sum',       # Count of pools with significant volume
    'token': 'count'                  # Number of active pools
}).reset_index()

# Rename columns for clarity
daily_bridge_vol_all.columns = [
    'timestamp_',
    'total_volume',
    'tokens_with_significant_volume',
    'tokens_count'
]

# Calculate 7-day moving average for protocol
daily_bridge_vol_all['volume_7d_ma'] = daily_bridge_vol_all['total_volume'].rolling(
    window=7, min_periods=1
).mean()

# Calculate protocol-wide growth rate
daily_bridge_vol_all['growth_rate'] = daily_bridge_vol_all['total_volume'].pct_change()

# Identify significant volume days for protocol (> 90th percentile)
threshold = daily_bridge_vol_all['total_volume'].quantile(0.9)
daily_bridge_vol_all['is_significant_volume_day'] = daily_bridge_vol_all['total_volume'] > threshold

# Add some additional protocol metrics
daily_bridge_vol_all['avg_volume_per_token'] = (
    daily_bridge_vol_all['total_volume'] / daily_bridge_vol_all['tokens_count']
)

daily_bridge_vol_all

# testing
combined_vol_all = combined.sort_values('timestamp_')
vol_df = combined_vol_all.copy()

vol_df.columns

daily_vol_all = vol_df.groupby(['timestamp_']).agg(
        # TVL metrics (end of day values)
        volume = ('volume', 'last')
    ).reset_index()

daily_vol_all['volume_7d_ma'] = daily_vol_all['volume'].rolling(window=7).mean()
daily_vol_all['volume_30d_ma'] = daily_vol_all['volume'].rolling(window=30).mean()

daily_vol_all['volume_change'] = daily_vol_all['volume'].pct_change()
daily_vol_all['volume_change_7d'] = daily_vol_all['volume_7d_ma'].pct_change()
daily_vol_all['volume_change_30d'] = daily_vol_all['volume_30d_ma'].pct_change()

daily_vol_all['is_significant_volume'] = daily_vol_all['volume'].transform(lambda x: x > x.quantile(0.9))

daily_vol_all = daily_vol_all.fillna(0)

daily_vol_all['volume'].iloc[-1]



# test
# Group by date and token

combined['date'] = pd.to_datetime(combined['timestamp_'])

deposits_by_token = combined[combined['type'] == 'deposit'].groupby(['date', 'token']).agg({
    'amount_usd': 'sum',
    'amount': 'sum',
    'transactionHash_': 'count'
}).rename(columns={
    'amount_usd': 'deposit_volume_usd',
    'amount': 'deposit_amount',
    'transactionHash_': 'deposit_count'
})

withdrawals_by_token = combined[combined['type'] == 'withdrawal'].groupby(['date', 'token']).agg({
    'amount_usd': 'sum',
    'amount': 'sum',
    'transactionHash_': 'count'
}).rename(columns={
    'amount_usd': 'withdrawal_volume_usd',
    'amount': 'withdrawal_amount',
    'transactionHash_': 'withdrawal_count'
})

# Combine
daily_by_token = pd.merge(
    deposits_by_token, 
    withdrawals_by_token, 
    left_index=True, 
    right_index=True, 
    how='outer'
).fillna(0)

# Calculate metrics
daily_by_token['net_flow_usd'] = daily_by_token['deposit_volume_usd'] - daily_by_token['withdrawal_volume_usd']
daily_by_token['net_flow_amount'] = daily_by_token['deposit_amount'] - daily_by_token['withdrawal_amount']
daily_by_token['total_volume_usd'] = daily_by_token['deposit_volume_usd'] + daily_by_token['withdrawal_volume_usd']

# Calculate cumulative TVL by token
daily_by_token['tvl_usd'] = daily_by_token.groupby(level='token')['net_flow_usd'].cumsum()
daily_by_token['tvl_amount'] = daily_by_token.groupby(level='token')['net_flow_amount'].cumsum()

daily_by_token = daily_by_token.reset_index()

combined
combined.columns

usdc = combined.loc[
    combined['token']=='USDC'][[
        'timestamp_', 'net_flow', 
        'deposit_amount_usd', 'withdrawal_amount_usd']]

usdc

tvl_by_token = combined.groupby(['token', 'timestamp_']).agg({
            'net_flow': 'sum',
            'deposit_amount_usd': 'sum',
            'withdrawal_amount_usd': 'sum'
            })

tvl_by_token['tvl'] = 