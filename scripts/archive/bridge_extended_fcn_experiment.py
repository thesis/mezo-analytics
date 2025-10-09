import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
from dotenv import load_dotenv

from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from mezo.currency_utils import format_currency_columns, replace_token_labels
from mezo.currency_config import TOKEN_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import get_token_prices
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Converting tokens to USD")
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

@with_progress("Cleaning bridge data")
def clean_bridge_data(raw, sort_col, date_cols, currency_cols, asset_col, txn_type):
    """Clean and format bridge transaction data."""
    if not ExceptionHandler.validate_dataframe(raw, "Raw bridge data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col)
    df = replace_token_labels(df, TOKEN_MAP)
    df = format_datetimes(df, date_cols)
    df = format_currency_columns(df, currency_cols, asset_col)

    df['type'] = txn_type

    return df


def calculate_growth_rate(series: pd.Series, days: int) -> float:
    """Calculate percentage growth over specified days"""
    if len(series) < days + 1:
        return 0
    current = series.iloc[-1]
    past = series.iloc[-days-1]
    if past == 0:
        return 0
    return ((current - past) / past) * 100

def calculate_max_drawdown(series: pd.Series) -> float:
    """Calculate maximum percentage drawdown"""
    if len(series) == 0:
        return 0
    running_max = series.expanding().max()
    drawdown = (series - running_max) / running_max * 100
    return abs(drawdown.min())

def calculate_consecutive_outflow_days(daily_df: pd.DataFrame) -> int:
    """Count consecutive days of net outflows"""
    if 'net_flow' not in daily_df.columns:
        return 0
    
    consecutive = 0
    for i in range(len(daily_df) - 1, -1, -1):
        if daily_df['net_flow'].iloc[i] < 0:
            consecutive += 1
        else:
            break
    return consecutive

# ==================================================
# GET RAW BRIDGE DATA
# ==================================================

# Load environment variables
load_dotenv(dotenv_path='../.env', override=True)
pd.options.display.float_format = '{:.5f}'.format

raw_deposits = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_SUBGRAPH,
    BridgeQueries.GET_BRIDGE_TRANSACTIONS,
    'assetsLockeds'
)

ProgressIndicators.print_step("Fetching raw bridge withdrawal data", "start")
raw_withdrawals = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH,
    BridgeQueries.GET_NATIVE_WITHDRAWALS,
    'assetsUnlockeds'
)

# ==================================================
# LOAD + CLEAN BRIDGE DATA
# ==================================================

deposits = clean_bridge_data(
    raw=raw_deposits, 
    sort_col='timestamp_',
    date_cols=['timestamp_'], 
    currency_cols=['amount'], 
    asset_col='token', 
    txn_type='deposit'
)

deposits_with_usd = add_usd_conversions(
    deposits,
    token_column='token',
    tokens_id_map=TOKENS_ID_MAP,
    amount_columns =['amount']
)

deposits_clean = deposits_with_usd[[
    'timestamp_', 'amount', 'token', 'amount_usd',
    'recipient', 'transactionHash_', 'type'
]]
deposits_clean = deposits_clean.rename(columns={'recipient': 'depositor'})

# clean withdrawals data
withdrawals = clean_bridge_data(
    raw_withdrawals, 
    sort_col='timestamp_',
    date_cols=['timestamp_'], 
    currency_cols=['amount'], 
    asset_col='token',
    txn_type='withdrawal'
)

withdrawals_with_usd = add_usd_conversions(
    withdrawals,
    token_column='token',
    tokens_id_map=TOKENS_ID_MAP,
    amount_columns=['amount']
)

bridge_map = {'0': 'ethereum', '1': 'bitcoin'}
withdrawals_with_usd['chain'] = withdrawals_with_usd['chain'].map(bridge_map)

withdrawals_clean = withdrawals_with_usd[[
    'timestamp_', 'amount', 'token', 'amount_usd', 'chain',
    'recipient', 'sender', 'transactionHash_', 'type'
]]
withdrawals_clean = withdrawals_clean.rename(columns={'sender': 'withdrawer', 'recipient': 'withdraw_recipient'})

# ==================================================
# COMBINE DEPOSIT AND WITHDRAWAL DATA
# ==================================================

combined = pd.concat(
    [deposits_clean, withdrawals_clean], 
    ignore_index=True
).fillna(0)

combined = combined.sort_values('timestamp_').reset_index(drop=True)

# ==================================================
# CREATE BRIDGE VOLUME AND BRIDGE TVL DATAFRAMES
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

# ==================================================
# CORE METRIC CALCULATIONS
# ==================================================

def calculate_bridge_metrics(combined: pd.DataFrame):
    """
    Main function to calculate all volume metrics
    
    Args:
        combined: Cleaned bridge transaction data with deposits and withdrawals
        
    Returns:
        Dictionary containing all calculated metrics dataframes
    """
    
    # Ensure timestamp is datetime
    combined['timestamp_'] = pd.to_datetime(combined['timestamp_'])
    combined['date'] = combined['timestamp_']
    
    # Calculate all metrics
    print("📊 Calculating volume metrics...")
    
    # Time series metrics
    daily_overall = calculate_daily_metrics_overall(combined)
    daily_by_token = calculate_daily_metrics_by_token(combined)
    weekly_overall = calculate_weekly_metrics(daily_overall)
    monthly_overall = calculate_monthly_metrics(daily_overall)
    
    # Summary metrics
    summary_overall = calculate_summary_metrics_overall(combined, daily_overall)
    summary_by_token = calculate_summary_metrics_by_token(combined)
    
    # User metrics
    user_metrics = calculate_user_metrics(combined)
    user_metrics_by_token = calculate_user_metrics_by_token(combined)
    
    # Health indicators
    health_metrics = calculate_health_indicators(daily_overall, combined)
    
    return {
        'daily_overall': daily_overall,
        'daily_by_token': daily_by_token,
        'weekly_overall': weekly_overall,
        'monthly_overall': monthly_overall,
        'summary_overall': summary_overall,
        'summary_by_token': summary_by_token,
        'user_metrics': user_metrics,
        'user_metrics_by_token': user_metrics_by_token,
        'health_metrics': health_metrics
    }

# ==================================================
# TIME SERIES CALCULATIONS
# ==================================================

def calculate_daily_metrics_overall(df: pd.DataFrame):
    """
    Calculate daily aggregated metrics for all tokens combined
    """
    
    # Separate deposits and withdrawals
    deposits = df[df['type'] == 'deposit'].groupby('date').agg({
        'amount_usd': 'sum',
        'transactionHash_': 'count',
        'depositor': 'nunique'
    }).rename(columns={
        'amount_usd': 'deposit_volume',
        'transactionHash_': 'deposit_count',
        'depositor': 'unique_depositors'
    })
    
    withdrawals = df[df['type'] == 'withdrawal'].groupby('date').agg({
        'amount_usd': 'sum',
        'transactionHash_': 'count',
        'withdrawer': 'nunique'
    }).rename(columns={
        'amount_usd': 'withdrawal_volume',
        'transactionHash_': 'withdrawal_count',
        'withdrawer': 'unique_withdrawers'
    })
    
    # Combine and calculate additional metrics
    daily = pd.merge(deposits, withdrawals, left_index=True, right_index=True, how='outer').fillna(0)
    
    # Core flow metrics
    daily['total_volume'] = daily['deposit_volume'] + daily['withdrawal_volume']
    daily['net_flow'] = daily['deposit_volume'] - daily['withdrawal_volume']
    daily['flow_ratio'] = daily['deposit_volume'] / daily['withdrawal_volume'].replace(0, np.nan)
    
    # Moving averages (7-day and 30-day)
    for col in ['deposit_volume', 'withdrawal_volume', 'net_flow', 'total_volume']:
        daily[f'{col}_ma7'] = daily[col].rolling(window=7, min_periods=1).mean()
        daily[f'{col}_ma30'] = daily[col].rolling(window=30, min_periods=1).mean()
    
    # Cumulative metrics
    daily['cumulative_deposits'] = daily['deposit_volume'].cumsum()
    daily['cumulative_withdrawals'] = daily['withdrawal_volume'].cumsum()
    daily['tvl'] = daily['net_flow'].cumsum()
    
    # TVL changes
    daily['tvl_change'] = daily['tvl'].diff()
    daily['tvl_change_pct'] = daily['tvl'].pct_change() * 100
    
    # Activity metrics
    daily['total_transactions'] = daily['deposit_count'] + daily['withdrawal_count']
    daily['total_unique_users'] = daily['unique_depositors'] + daily['unique_withdrawers']
    daily['avg_transaction_size'] = daily['total_volume'] / daily['total_transactions'].replace(0, np.nan)
    
    # Volatility (rolling 7-day standard deviation of daily volumes)
    daily['volume_volatility_7d'] = daily['total_volume'].rolling(window=7, min_periods=1).std()
    
    return daily.round(2)

def calculate_daily_metrics_by_token(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily metrics broken down by token
    """
    
    # Group by date and token
    deposits_by_token = df[df['type'] == 'deposit'].groupby(['date', 'token']).agg({
        'amount_usd': 'sum',
        'amount': 'sum',
        'transactionHash_': 'count'
    }).rename(columns={
        'amount_usd': 'deposit_volume_usd',
        'amount': 'deposit_amount',
        'transactionHash_': 'deposit_count'
    })
    
    withdrawals_by_token = df[df['type'] == 'withdrawal'].groupby(['date', 'token']).agg({
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
    
    return daily_by_token.round(2)

def calculate_weekly_metrics(daily: pd.DataFrame):
    """
    Aggregate daily metrics to weekly level
    """
    # Convert index to datetime if needed
    weekly = daily.resample('W').agg({
        'deposit_volume': 'sum',
        'withdrawal_volume': 'sum',
        'net_flow': 'sum',
        'total_volume': 'sum',
        'tvl': 'last',  # Take end-of-week TVL
        'deposit_count': 'sum',
        'withdrawal_count': 'sum',
        'total_transactions': 'sum',
        'unique_depositors': 'sum',
        'unique_withdrawers': 'sum'
    })
    
    # Calculate week-over-week changes
    weekly['wow_volume_change'] = weekly['total_volume'].pct_change() * 100
    weekly['wow_tvl_change'] = weekly['tvl'].pct_change() * 100
    weekly['avg_daily_volume'] = weekly['total_volume'] / 7
    
    return weekly.round(2)

def calculate_monthly_metrics(daily: pd.DataFrame):
    """
    Aggregate daily metrics to monthly level
    """
    
    monthly = daily.resample('M').agg({
        'deposit_volume': 'sum',
        'withdrawal_volume': 'sum',
        'net_flow': 'sum',
        'total_volume': 'sum',
        'tvl': 'last',
        'deposit_count': 'sum',
        'withdrawal_count': 'sum',
        'total_transactions': 'sum',
        'unique_depositors': 'sum',
        'unique_withdrawers': 'sum'
    })
    
    # Calculate month-over-month changes
    monthly['mom_volume_change'] = monthly['total_volume'].pct_change() * 100
    monthly['mom_tvl_change'] = monthly['tvl'].pct_change() * 100
    
    return monthly.round(2)

# ==================================================
# SUMMARY METRICS
# ==================================================

def calculate_summary_metrics_overall(df: pd.DataFrame, daily_df: pd.DataFrame):
    """
    Calculate overall summary statistics
    """
    
    current_date = df['date'].max()
    days_since_launch = (current_date - df['date'].min()).days
    
    # Get recent periods
    last_24h = df[df['date'] == current_date]
    last_7d = df[df['date'] >= current_date - timedelta(days=7)]
    last_30d = df[df['date'] >= current_date - timedelta(days=30)]
    
    summary = pd.DataFrame([{
        # Current state
        'current_tvl': daily_df['tvl'].iloc[-1],
        'tvl_ath': daily_df['tvl'].max(),
        'tvl_ath_date': daily_df['tvl'].idxmax(),
        'distance_from_ath_pct': ((daily_df['tvl'].iloc[-1] / daily_df['tvl'].max()) - 1) * 100,
        
        # Volume metrics
        'volume_24h': last_24h['amount_usd'].sum(),
        'volume_7d': last_7d['amount_usd'].sum(),
        'volume_30d': last_30d['amount_usd'].sum(),
        'total_volume_all_time': df['amount_usd'].sum(),
        
        # Transaction metrics
        'transactions_24h': len(last_24h),
        'deposits_24h': daily_df['deposit_count'].iloc[-1],
        'withdrawals_24h': daily_df['withdrawal_count'].iloc[-1],

        'transactions_7d': len(last_7d),
        'transactions_30d': len(last_30d),

        'total_deposits_all_time': daily_df['deposit_count'].sum(),
        'total_withdrawals_all_time': daily_df['withdrawal_count'].sum(),
        'total_transactions_all_time': len(df), 
        
        # User metrics
        'unique_users_24h': pd.concat([last_24h['depositor'], last_24h['withdrawer']]).nunique(),
        'unique_users_7d': pd.concat([last_7d['depositor'], last_7d['withdrawer']]).nunique(),
        'unique_users_30d': pd.concat([last_30d['depositor'], last_30d['withdrawer']]).nunique(),
        'total_unique_users_all_time': pd.concat([df['depositor'], df['withdrawer']]).nunique(),
        
        # Average metrics
        'avg_transaction_size_24h': last_24h['amount_usd'].mean() if len(last_24h) > 0 else 0,
        'avg_transaction_size_7d': last_7d['amount_usd'].mean() if len(last_7d) > 0 else 0,
        'avg_transaction_size_all_time': df['amount_usd'].mean(),
        
        # Flow metrics
        'inflow_24h': last_24h[last_24h['type'] == 'deposit']['amount_usd'].sum(),
        'outflow_24h': last_24h[last_24h['type'] == 'withdrawal']['amount_usd'].sum(),
        'net_flow_24h': last_24h[last_24h['type'] == 'deposit']['amount_usd'].sum() - 
                        last_24h[last_24h['type'] == 'withdrawal']['amount_usd'].sum(),

        'inflow_7d': last_7d[last_7d['type'] == 'deposit']['amount_usd'].sum(),
        'outflow_7d': last_7d[last_7d['type'] == 'withdrawal']['amount_usd'].sum(),
        'net_flow_7d': last_7d[last_7d['type'] == 'deposit']['amount_usd'].sum() - 
                    last_7d[last_7d['type'] == 'withdrawal']['amount_usd'].sum(),

        'net_flow_30d': last_30d[last_30d['type'] == 'deposit']['amount_usd'].sum() - 
                        last_30d[last_30d['type'] == 'withdrawal']['amount_usd'].sum(),
        
        # Growth rates
        'tvl_growth_7d_pct': calculate_growth_rate(daily_df['tvl'], 7),
        'tvl_growth_30d_pct': calculate_growth_rate(daily_df['tvl'], 30),
        'volume_growth_7d_pct': calculate_growth_rate(daily_df['total_volume'].rolling(7).sum(), 7),
        
        # Other
        'days_since_launch': days_since_launch,
        'avg_daily_volume': df['amount_usd'].sum() / days_since_launch
    }])
    
    return summary.round(2)

def calculate_summary_metrics_by_token(df: pd.DataFrame):
    """
    Calculate summary metrics for each token
    """
    
    # Separate deposits and withdrawals for each token
    token_summary = []
    
    for token in df['token'].unique():
        if pd.isna(token) or token == 0:
            continue
            
        token_data = df[df['token'] == token]
        deposits = token_data[token_data['type'] == 'deposit']
        withdrawals = token_data[token_data['type'] == 'withdrawal']
        
        # Calculate token type
        token_type = TOKEN_TYPE_MAP.get(token, 'unknown')
        
        summary = {
            'token': token,
            'token_type': token_type,
            
            # Volume metrics
            'total_deposit_volume': deposits['amount_usd'].sum(),
            'total_withdrawal_volume': withdrawals['amount_usd'].sum(),
            'net_volume': deposits['amount_usd'].sum() - withdrawals['amount_usd'].sum(),
            'total_volume': token_data['amount_usd'].sum(),
            
            # Amount metrics (native units)
            'total_deposit_amount': deposits['amount'].sum(),
            'total_withdrawal_amount': withdrawals['amount'].sum(),
            'net_amount': deposits['amount'].sum() - withdrawals['amount'].sum(),
            
            # Transaction counts
            'deposit_count': len(deposits),
            'withdrawal_count': len(withdrawals),
            'total_transactions': len(token_data),
            
            # User metrics
            'unique_depositors': deposits['depositor'].nunique(),
            'unique_withdrawers': withdrawals['withdrawer'].nunique(),
            'total_unique_users': pd.concat([deposits['depositor'], withdrawals['withdrawer']]).nunique(),
            
            # Average metrics
            'avg_deposit_size': deposits['amount_usd'].mean() if len(deposits) > 0 else 0,
            'avg_withdrawal_size': withdrawals['amount_usd'].mean() if len(withdrawals) > 0 else 0,
            'avg_transaction_size': token_data['amount_usd'].mean(),
            
            # Time metrics
            'first_transaction': token_data['timestamp_'].min(),
            'last_transaction': token_data['timestamp_'].max(),
            'days_active': (token_data['timestamp_'].max() - token_data['timestamp_'].min()).days,
            
            # Dominance
            'volume_share_pct': (token_data['amount_usd'].sum() / df['amount_usd'].sum()) * 100
        }
        
        token_summary.append(summary)
    
    summary_df = pd.DataFrame(token_summary)
    
    # Sort by total volume
    summary_df = summary_df.sort_values('total_volume', ascending=False)
    
    return summary_df.round(2)

# ==================================================
# USER METRICS
# ==================================================

def calculate_user_metrics(df: pd.DataFrame):
    """
    Calculate user behavior metrics
    """
    
    # Combine all users
    all_users = pd.concat([
        df[['depositor', 'amount_usd', 'timestamp_', 'type']].rename(columns={'depositor': 'user'}),
        df[['withdrawer', 'amount_usd', 'timestamp_', 'type']].rename(columns={'withdrawer': 'user'})
    ])
    
    # Remove null users
    all_users = all_users[all_users['user'].notna()]
    
    user_metrics = all_users.groupby('user').agg({
        'amount_usd': ['sum', 'mean', 'count', 'max'],
        'timestamp_': ['min', 'max'],
        'type': lambda x: x.value_counts().to_dict()
    })
    
    user_metrics.columns = ['total_volume', 'avg_transaction', 'transaction_count', 
                            'largest_transaction', 'first_activity', 'last_activity', 'activity_breakdown']
    
    # Calculate additional metrics
    user_metrics['days_active'] = (user_metrics['last_activity'] - user_metrics['first_activity']).dt.days
    user_metrics['is_active_30d'] = user_metrics['last_activity'] >= (datetime.now() - timedelta(days=30))
    
    # User segments based on volume
    user_metrics['segment'] = pd.cut(
        user_metrics['total_volume'],
        bins=[0, 1000, 10000, 100000, float('inf')],
        labels=['Retail (<$1k)', 'Mid ($1k-10k)', 'Large ($10k-100k)', 'Whale (>$100k)']
    )
    
    return user_metrics

def calculate_user_metrics_by_token(df: pd.DataFrame):
    """
    Calculate user metrics broken down by token
    """
    
    user_token_metrics = []
    
    for token in df['token'].unique():
        if pd.isna(token) or token == 0:
            continue
            
        token_data = df[df['token'] == token]
        
        # Combine users for this token
        token_users = pd.concat([
            token_data['depositor'],
            token_data['withdrawer']
        ]).dropna().unique()
        
        metrics = {
            'token': token,
            'unique_users': len(token_users),
            'avg_user_volume': token_data.groupby('depositor')['amount_usd'].sum().mean(),
            'median_user_volume': token_data.groupby('depositor')['amount_usd'].sum().median(),
            'whale_concentration': token_data.nlargest(10, 'amount_usd')['amount_usd'].sum() / token_data['amount_usd'].sum() * 100
        }
        
        user_token_metrics.append(metrics)
    
    return pd.DataFrame(user_token_metrics).round(2)

# ==================================================
# HEALTH INDICATORS
# ==================================================

def calculate_health_indicators(daily_df: pd.DataFrame, df: pd.DataFrame):
    """
    Calculate bridge health and risk indicators
    """
    
    health = pd.DataFrame([{
        # Liquidity health
        'current_tvl': daily_df['tvl'].iloc[-1],
        'tvl_volatility_30d': daily_df['tvl_change_pct'].iloc[-30:].std() if len(daily_df) >= 30 else 0,
        'max_drawdown_30d': calculate_max_drawdown(daily_df['tvl'].iloc[-30:]) if len(daily_df) >= 30 else 0,
        
        # Flow health
        'consecutive_outflow_days': calculate_consecutive_outflow_days(daily_df),
        'outflow_ratio_7d': daily_df['withdrawal_volume'].iloc[-7:].sum() / 
                            daily_df['deposit_volume'].iloc[-7:].sum() if daily_df['deposit_volume'].iloc[-7:].sum() > 0 else 0,
        
        # Concentration risk
        'largest_transaction': df['amount_usd'].max(),
        'largest_tx_pct_of_tvl': (df['amount_usd'].max() / daily_df['tvl'].iloc[-1]) * 100 if daily_df['tvl'].iloc[-1] > 0 else 0,
        'whale_concentration_pct': df.nlargest(10, 'amount_usd')['amount_usd'].sum() / df['amount_usd'].sum() * 100,
        
        # Activity health
        'daily_active_users_7d_avg': daily_df['total_unique_users'].iloc[-7:].mean() if len(daily_df) >= 7 else 0,
        'daily_volume_7d_avg': daily_df['total_volume'].iloc[-7:].mean() if len(daily_df) >= 7 else 0,
        'avg_transaction_size_7d': df[df['date'] >= df['date'].max() - timedelta(days=7)]['amount_usd'].mean(),
        
        # Momentum indicators
        'volume_momentum_7d': (daily_df['total_volume_ma7'].iloc[-1] - daily_df['total_volume_ma30'].iloc[-1]) 
                            if len(daily_df) >= 30 else 0,
        'tvl_trend': 'growing' if daily_df['tvl_change'].iloc[-7:].mean() > 0 else 'declining'
    }])
    
    # Add risk level assessment
    risk_score = 0
    if health['tvl_volatility_30d'].iloc[0] > 15:
        risk_score += 2
    if health['consecutive_outflow_days'].iloc[0] > 3:
        risk_score += 2
    if health['whale_concentration_pct'].iloc[0] > 50:
        risk_score += 1
    if health['outflow_ratio_7d'].iloc[0] > 1.2:
        risk_score += 1
    
    health['risk_level'] = 'LOW' if risk_score <= 2 else 'MEDIUM' if risk_score <= 4 else 'HIGH'
    health['risk_score'] = risk_score
    
    return health.round(2)

# ==================================================
# DISPLAY FUNCTIONS
# ==================================================

def display_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of key metrics"""
    
    print("\n" + "="*60)
    print("📊 BRIDGE VOLUME METRICS SUMMARY")
    print("="*60)
    
    # Overall summary
    summary = metrics['summary_overall'].iloc[0]
    print(f"\n💰 TVL: ${summary['current_tvl']:,.0f} ({summary['tvl_growth_7d_pct']:+.1f}% 7d)")
    print(f"💱 Net Flow (7d): ${summary['net_flow_7d']:,.0f}")
    
    # Calculate deposit/withdrawal metrics for each timeframe
    current_date = df['date'].max()
    
    # Define timeframes
    timeframes = {
        '24h': df[df['date'] == current_date],
        '7d': df[df['date'] >= current_date - timedelta(days=7)],
        '30d': df[df['date'] >= current_date - timedelta(days=30)],
        'All-time': df
    }
    
    # Print deposit and withdrawal metrics
    print("\n📥 DEPOSITS:")
    print("-" * 50)
    print(f"{'Period':<12} {'Count':>10} {'Amount':>20}")
    print("-" * 50)
    
    for period_name, period_data in timeframes.items():
        deposits = period_data[period_data['type'] == 'deposit']
        deposit_count = len(deposits)
        deposit_amount = deposits['amount_usd'].sum()
        print(f"{period_name:<12} {deposit_count:>10,} ${deposit_amount:>18,.0f}")
    
    print("\n📤 WITHDRAWALS:")
    print("-" * 50)
    print(f"{'Period':<12} {'Count':>10} {'Amount':>20}")
    print("-" * 50)
    
    for period_name, period_data in timeframes.items():
        withdrawals = period_data[period_data['type'] == 'withdrawal']
        withdrawal_count = len(withdrawals)
        withdrawal_amount = withdrawals['amount_usd'].sum()
        print(f"{period_name:<12} {withdrawal_count:>10,} ${withdrawal_amount:>18,.0f}")
    
    print("\n💱 NET FLOW:")
    print("-" * 50)
    print(f"{'Period':<12} {'Net Count':>10} {'Net Amount':>20}")
    print("-" * 50)
    
    for period_name, period_data in timeframes.items():
        deposits = period_data[period_data['type'] == 'deposit']
        withdrawals = period_data[period_data['type'] == 'withdrawal']
        net_count = len(deposits) - len(withdrawals)
        net_amount = deposits['amount_usd'].sum() - withdrawals['amount_usd'].sum()
        
        # Add + sign for positive values
        count_sign = "+" if net_count > 0 else ""
        amount_sign = "+" if net_amount > 0 else ""
        
        print(f"{period_name:<12} {count_sign}{net_count:>9,} {amount_sign}${abs(net_amount):>17,.0f}")
    
    # Top tokens
    print("\n🪙 TOP TOKENS BY VOLUME:")
    print("-" * 50)
    top_tokens = metrics['summary_by_token'].head(5)[['token', 'total_volume', 'volume_share_pct', 'net_volume']]
    for _, row in top_tokens.iterrows():
        print(f"  {row['token']:<8} ${row['total_volume']:>12,.0f} ({row['volume_share_pct']:>5.1f}%) | Net: ${row['net_volume']:>12,.0f}")
    
    # User metrics
    print("\n👥 USER ACTIVITY:")
    print("-" * 50)
    print(f"  24h Active Users:    {summary['unique_users_24h']:>8.0f}")
    print(f"  7d Active Users:     {summary['unique_users_7d']:>8.0f}")
    print(f"  30d Active Users:    {summary['unique_users_30d']:>8.0f}")
    print(f"  All-time Users:      {summary['total_unique_users_all_time']:>8.0f}")
    
    # Health indicators
    health = metrics['health_metrics'].iloc[0]
    print(f"\n🏥 HEALTH STATUS: {health['risk_level']} (Score: {health['risk_score']}/6)")
    print("-" * 50)
    print(f"  Volatility (30d):           {health['tvl_volatility_30d']:>6.1f}%")
    print(f"  Max Drawdown (30d):         {health['max_drawdown_30d']:>6.1f}%")
    print(f"  Consecutive Outflow Days:   {health['consecutive_outflow_days']:>6.0f}")
    print(f"  Whale Concentration:        {health['whale_concentration_pct']:>6.1f}%")
    print(f"  Outflow Ratio (7d):         {health['outflow_ratio_7d']:>6.2f}")

# ==================================================
# EXPORT FUNCTIONS
# ==================================================

def export_to_csv(metrics: Dict[str, pd.DataFrame], prefix: str = "bridge_metrics"):
    """Export all metrics to CSV files"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for name, df in metrics.items():
        filename = f"{prefix}_{name}_{timestamp}.csv"
        df.to_csv(filename)
        print(f"✅ Exported {name} to {filename}")

# ==================================================
# MAIN EXECUTION
# ==================================================

from mezo.currency_config import TOKEN_TYPE_MAP
    
# Calculate all metrics
metrics = calculate_bridge_metrics(combined)

# Access specific dataframes
daily_overall = metrics['daily_overall']
token_summary = metrics['summary_by_token']
health = metrics['health_metrics']

# Display summary
display_summary(metrics, combined)

export_to_csv(metrics)

datetime.now()