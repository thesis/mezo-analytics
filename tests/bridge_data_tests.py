from dotenv import load_dotenv
import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from mezo.currency_utils import format_currency_columns, replace_token_labels
from mezo.currency_config import TOKEN_MAP, TOKEN_TYPE_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import get_token_prices
from mezo.data_utils import add_rolling_values
from mezo.clients import SupabaseClient, BigQueryClient
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
import numpy as np

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
# ========== ========================================

raw_data = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_SUBGRAPH, 
    BridgeQueries.GET_BRIDGE_TRANSACTIONS,
    'assetsLockeds'
)

raw_withdrawals = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH, 
    BridgeQueries.GET_NATIVE_WITHDRAWALS,
    'assetsUnlockeds'
)

raw_data.to_csv('test_deposits.csv')
raw_withdrawals.to_csv('test_withdrawals.csv')

raw_withdrawals = pd.read_csv('test_withdrawals.csv')
raw_data = pd.read_csv('test_deposits.csv')


# ==================================================
# CLEAN BRIDGE DATA
# ==================================================

# Clean deposits data
deposits = clean_bridge_data(raw_data, 'timestamp_', ['timestamp_'], ['amount'], 'token')
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

withdrawals_clean = withdrawals_clean.rename(columns={'sender': 'withdrawer', 'recipient': 'withdraw_recipient'})

# ==================================================
# COMBINE DEPOSIT AND WITHDRAWAL DATA
# ==================================================

# Combine data
combined = pd.concat([deposits_clean, withdrawals_clean], ignore_index=True)

combined = combined.sort_values(['token', 'timestamp_']).reset_index(drop=True)
combined = combined.fillna(0)

combined['volume'] = combined['amount_usd'].cumsum()

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
combined['volume_total'] = combined['amount_usd'].cumsum()
combined['volume'] = combined['withdrawal_amount_usd'] + combined['deposit_amount_usd']

bridge_volume = combined[[
    'timestamp_', 'amount', 'token', 'amount_usd', 
    'type', 'chain', 'volume', 'transactionHash_'
]]

combined['tvl'] = combined.groupby('token')['net_flow'].cumsum()

bridge_flow = combined[[
    'timestamp_', 'amount', 'token', 'amount_usd', 
    'type', 'chain', 'net_flow', 'transactionHash_'
]]


# user_stats = combined.groupby('depositor').agg({
#     'amount_usd': ['sum', 'mean', 'count'],
#     'timestamp_': ['min', 'max'],
#     'token': lambda x: x.nunique()
# })

# user_stats.columns = ['total_volume', 'avg_transaction', 'transaction_count', 
#                           'first_bridge', 'last_bridge', 'unique_tokens']
    
# # User segments
# user_segments = pd.cut(user_stats['total_volume'], 
#                         bins=[0, 1000, 10000, 100000, float('inf')],
#                         labels=['Retail', 'Mid', 'Large', 'Whale'])

# user_stats['last_bridge'] = pd.to_datetime(user_stats['last_bridge'])
# user_stats['first_bridge'] = pd.to_datetime(user_stats['first_bridge'])


# # Retention metrics
# user_stats['days_active'] = (user_stats['last_bridge'] - user_stats['first_bridge']).dt.days
# user_stats['is_repeat'] = user_stats['transaction_count'] > 1

# print({
#         'total_users': len(user_stats),
#         'repeat_users': user_stats['is_repeat'].sum(),
#         'retention_rate': user_stats['is_repeat'].mean() * 100,
#         'avg_user_volume': user_stats['total_volume'].mean(),
#         'whale_concentration': user_stats.nlargest(10, 'total_volume')['total_volume'].sum() / user_stats['total_volume'].sum() * 100,
#         'user_segments': user_segments.value_counts()
#     })

# ========================================
# AGGREGATE BRIDGE VOLUME STATS (PER TOKEN)
# ========================================

combined_tkn_time = combined.copy()
combined_tkn_time = combined_tkn_time.sort_values(['token', 'timestamp_']).reset_index(drop=True)
combined_tkn_time = combined_tkn_time.fillna(0)
combined_tkn_time['volume'] = combined_tkn_time['amount_usd'].cumsum()

bridge_volume = combined_tkn_time[[
    'timestamp_', 'amount', 'token', 'amount_usd', 
    'type', 'chain', 'volume', 'transactionHash_'
]]


# Make a copy to avoid modifying original
df = bridge_volume.copy()

# Ensure timestamp is datetime and create date column
df['timestamp_'] = pd.to_datetime(df['timestamp_'])
df['timestamp_'] = df['timestamp_'].dt.date

# Sort by pool and timestamp for proper calculations
# df = df.sort_values(['token', 'timestamp_'])

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

# ========================================
# AGGREGATE NET FLOW AND TVL STATS (PER TOKEN)
# ========================================

tvl_df = combined.copy()

tvl_df.columns

tvl_df.tail(20)

daily_tvl_by_token = tvl_df.groupby(['token', 'timestamp_']).agg({
        # TVL metrics (end of day values)
        'tvl': 'last',
        'net_flow': 'sum',  # Net daily flow
        'deposit_amount_usd': 'sum',  # Total daily deposits
        'withdrawal_amount_usd': 'sum',  # Total daily withdrawals
        'type': 'count',  # Total transactions
    }).reset_index()

# Calculate deposit and withdrawal counts
deposit_counts = combined[combined['type'] == 'deposit'].groupby(['token', 'timestamp_']).size()
depositors = combined[combined['type'] == 'deposit'].groupby(['token', 'timestamp_'])['depositor'].nunique()
withdrawal_counts = combined[combined['type'] == 'withdrawal'].groupby(['token', 'timestamp_']).size()
withdrawers = combined[combined['type'] == 'withdrawal'].groupby(['token', 'timestamp_'])['withdrawer'].nunique()
    
# Add transaction counts to daily metrics
daily_tvl = daily_tvl_by_token.set_index(['token', 'timestamp_'])
daily_tvl['deposits'] = deposit_counts
daily_tvl['unique_depositors'] = depositors
daily_tvl['withdrawals'] = withdrawal_counts
daily_tvl['unique_withdrawers'] = withdrawers
daily_tvl['unique_users'] = daily_tvl['unique_withdrawers'] + daily_tvl['unique_depositors']
daily_tvl['total_transactions'] = daily_tvl['deposits'] + daily_tvl['withdrawals']
daily_tvl = daily_tvl.fillna(0).reset_index()

# # Rename columns for clarity
# daily_tvl.columns = [
#     'timestamp_', 'token', 'tvl', 'net_flow', 'deposits_usd', 'withdrawals_usd',
#     'total_transactions', 'unique_depositors','deposits', 'withdrawals', 'unique_withdrawers', 'unique_users'
# ]

daily_tvl.columns

# Calculate additional metrics
daily_tvl['deposit_withdrawal_ratio'] = np.where(
    daily_tvl['withdrawal_amount_usd'] > 0,
    daily_tvl['deposit_amount_usd'] / daily_tvl['withdrawal_amount_usd'],
    np.inf
)
    
daily_tvl['tvl_change'] = daily_tvl.groupby('token')['tvl'].diff()
daily_tvl['tvl_change_pct'] = daily_tvl.groupby('token')['tvl'].pct_change()
    
# Calculate 7-day moving averages for key metrics
add_rolling_values(daily_tvl, 7, ['tvl', 'deposit_amount_usd', 'withdrawal_amount_usd', 'net_flow'])


daily_tvl[['timestamp_', 'token', 'tvl']].tail(30)

# =========================================
# GET TOTAL DAILY TVL AND FLOW (ALL TOKENS)
# =========================================

tvl_df = tvl_df.sort_values(['token', 'timestamp_'])
daily_tvl_all = tvl_df.groupby(['timestamp_']).agg({
        # TVL metrics (end of day values)
        'net_flow': 'sum',  # Net daily flow
        'deposit_amount_usd': 'sum',  # Total daily deposits
        'withdrawal_amount_usd': 'sum',  # Total daily withdrawals
        'type': 'count',  # Total transactions
    }).reset_index()

daily_tvl_all['tvl'] = daily_tvl_all['net_flow'].cumsum()

# Calculate deposit and withdrawal counts
deposit_counts = combined[combined['type'] == 'deposit'].groupby(['timestamp_']).size()
depositors = combined[combined['type'] == 'deposit'].groupby(['timestamp_'])['depositor'].nunique()
withdrawal_counts = combined[combined['type'] == 'withdrawal'].groupby(['timestamp_']).size()
withdrawers = combined[combined['type'] == 'withdrawal'].groupby(['timestamp_'])['withdrawer'].nunique()
    
# Add transaction counts to daily metrics
daily_tvl_all_final = daily_tvl_all.set_index(['timestamp_'])
daily_tvl_all_final['deposits'] = deposit_counts
daily_tvl_all_final['unique_depositors'] = depositors
daily_tvl_all_final['withdrawals'] = withdrawal_counts
daily_tvl_all_final['unique_withdrawers'] = withdrawers
daily_tvl_all_final['unique_users'] = daily_tvl_all_final['unique_withdrawers'] + daily_tvl_all_final['unique_depositors']
daily_tvl_all_final['total_transactions'] = daily_tvl_all_final['deposits'] + daily_tvl_all_final['withdrawals']
daily_tvl_all_final = daily_tvl_all_final.fillna(0).reset_index()

# df_daily

# df_pivot = df_daily.pivot(
#     index='timestamp_', columns='token'
# ).fillna(0)

# # Flatten column names
# df_pivot.columns = [
#     '_'.join(col).strip() for col in df_pivot.columns.values
# ]
# daily_tvl_test = df_pivot.reset_index()
        
# Sort by timestamp for proper cumulative calculations

daily_protocol_metrics = daily_tvl.groupby('timestamp_').agg({
        # TVL metrics (sum across all pools)
        'tvl': 'last',
        'net_flow': 'sum',
        'deposit_amount_usd': 'sum',
        'withdrawal_amount_usd': 'sum',
        'total_transactions': 'sum',
        'deposits': 'sum',
        'withdrawals': 'sum',
        'unique_users': 'sum',  # Note: might count same user across pools
        'token': 'count'  # Number of active pools
    }).reset_index()

daily_protocol_metrics['tvl'].sum()

deposits_clean['amount_usd'].sum() - withdrawals_clean['amount_usd'].sum()
    
# Calculate protocol-wide additional metrics
daily_protocol_metrics['protocol_deposit_withdrawal_ratio'] = np.where(
    daily_protocol_metrics['withdrawals'] > 0,
    daily_protocol_metrics['deposits'] / daily_protocol_metrics['withdrawals'],
    np.inf
)
    
daily_protocol_metrics['tvl_change'] = daily_protocol_metrics['tvl'].diff()
daily_protocol_metrics['tvl_change_pct'] = daily_protocol_metrics['tvl'].pct_change()
    
# Calculate 7-day moving averages for protocol metrics
add_rolling_values(daily_protocol_metrics, 7, ['tvl', 'deposits_usd', 'withdrawals_usd', 'net_flow'])

# Identify high activity days
daily_protocol_metrics['high_deposit_day'] = daily_protocol_metrics['deposits'] > daily_protocol_metrics['deposits'].quantile(0.9)
daily_protocol_metrics['high_withdrawal_day'] = daily_protocol_metrics['withdrawals'] > daily_protocol_metrics['withdrawals'].quantile(0.9)

# Calculate average metrics per pool
daily_protocol_metrics['avg_tvl_per_token'] = daily_protocol_metrics['tvl'] / daily_protocol_metrics['token']
daily_protocol_metrics['avg_deposits_per_token'] = daily_protocol_metrics['deposits'] / daily_protocol_metrics['token']
daily_protocol_metrics['avg_withdrawals_per_token'] = daily_protocol_metrics['withdrawals'] / daily_protocol_metrics['token']