from datetime import timedelta, datetime, date
from typing import Dict
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import sys
import os

from mezo.clients import SubgraphClient, BigQueryClient
from mezo.queries import BridgeQueries
from mezo.currency_config import TOKEN_TYPE_MAP
from mezo.test_utils import tests
from mezo.currency_utils import Conversions
from mezo.datetime_utils import format_datetimes
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

# Add reports directory to path for report generation
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from reports.generate_summary_reports import ReportGenerator

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Cleaning bridge data")
def clean_bridge_data(raw, sort_col, date_cols, currency_cols, asset_col, txn_type):
    """Clean and format bridge transaction data."""
    conversions = Conversions()
    
    if not ExceptionHandler.validate_dataframe(raw, "Raw bridge data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col)
    df = conversions.replace_token_addresses_with_symbols(df=df)
    df = format_datetimes(df, date_cols)
    df = conversions.format_token_decimals(df, currency_cols, asset_col)
    # format(df, currency_cols, asset_col)

    df['type'] = txn_type

    return df

@with_progress("Calculating growth rate")
def calculate_growth_rate(series: pd.Series, days: int):
    """Calculate percentage growth over specified days"""
    if len(series) < days + 1:
        return 0
    current = series.iloc[-1]
    past = series.iloc[-days-1]
    if past == 0:
        return 0
    return ((current - past) / past) * 100

@with_progress("Calculating max drawdown")
def calculate_max_drawdown(series: pd.Series):
    """Calculate maximum percentage drawdown"""
    if len(series) == 0:
        return 0
    running_max = series.expanding().max()
    drawdown = (series - running_max) / running_max * 100
    return abs(drawdown.min())

@with_progress("Calculating consecutive outflow days")
def calculate_consecutive_outflow_days(daily_df: pd.DataFrame):
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
    combined['date'] = pd.to_datetime(combined['timestamp_'])
    combined['timestamp_'] = pd.to_datetime(combined['timestamp_'])
    
    print("📊 Calculating metrics")
    
    # Time series metrics
    daily_overall = calculate_daily_metrics_overall(combined)
    daily_by_token = calculate_daily_metrics_by_token(combined)
    
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
        'summary_overall': summary_overall,
        'summary_by_token': summary_by_token,
        'user_metrics': user_metrics,
        'user_metrics_by_token': user_metrics_by_token,
        'health_metrics': health_metrics
    }

# TIME SERIES CALCULATIONS
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
        'amount_usd': 'deposit_amount_usd',
        'transactionHash_': 'deposit_count',
        'depositor': 'unique_depositors'
    })
    
    withdrawals = df[df['type'] == 'withdrawal'].groupby('date').agg({
        'amount_usd': 'sum',
        'transactionHash_': 'count',
        'withdrawer': 'nunique'
    }).rename(columns={
        'amount_usd': 'withdrawal_amount_usd',
        'transactionHash_': 'withdrawal_count',
        'withdrawer': 'unique_withdrawers'
    })
    
    # Combine and calculate additional metrics
    daily = pd.merge(deposits, withdrawals, left_index=True, right_index=True, how='outer').fillna(0)
    
    # Core flow metrics
    daily['total_volume'] = daily['deposit_amount_usd'] + daily['withdrawal_amount_usd']
    daily['net_flow'] = daily['deposit_amount_usd'] - daily['withdrawal_amount_usd']
    daily['flow_ratio'] = daily['deposit_amount_usd'] / daily['withdrawal_amount_usd'].replace(0, np.nan)
    
    # Moving averages (7-day and 30-day)
    for col in ['deposit_amount_usd', 'withdrawal_amount_usd', 'net_flow', 'total_volume']:
        daily[f'{col}_ma7'] = daily[col].rolling(window=7, min_periods=1).mean()
        daily[f'{col}_ma30'] = daily[col].rolling(window=30, min_periods=1).mean()
    
    # Cumulative metrics
    daily['cumulative_deposits'] = daily['deposit_amount_usd'].cumsum()
    daily['cumulative_withdrawals'] = daily['withdrawal_amount_usd'].cumsum()
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
    
    daily = daily.reset_index()

    return daily.round(2)

def calculate_daily_metrics_by_token(df: pd.DataFrame):
    """
    Calculate daily metrics broken down by token
    """
    
    # Group by date and token
    deposits_by_token = df[df['type'] == 'deposit'].groupby(['date', 'token']).agg({
        'amount_usd': 'sum',
        'amount': 'sum',
        'transactionHash_': 'count'
    }).rename(columns={
        'amount_usd': 'deposit_amount_usd',
        'amount': 'deposit_amount_native',
        'transactionHash_': 'deposit_count'
    })
    
    withdrawals_by_token = df[df['type'] == 'withdrawal'].groupby(['date', 'token']).agg({
        'amount_usd': 'sum',
        'amount': 'sum',
        'transactionHash_': 'count'
    }).rename(columns={
        'amount_usd': 'withdrawal_amount_usd',
        'amount': 'withdrawal_amount_native',
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
    daily_by_token['net_flow'] = daily_by_token['deposit_amount_usd'] - daily_by_token['withdrawal_amount_usd']
    daily_by_token['net_flow_native'] = daily_by_token['deposit_amount_native'] - daily_by_token['withdrawal_amount_native']
    daily_by_token['total_volume_usd'] = daily_by_token['deposit_amount_usd'] + daily_by_token['withdrawal_amount_usd']
    
    # Calculate cumulative TVL by token
    daily_by_token['tvl'] = daily_by_token.groupby(level='token')['net_flow'].cumsum()
    daily_by_token['tvl_native'] = daily_by_token.groupby(level='token')['net_flow_native'].cumsum()
    daily_by_token = daily_by_token.reset_index()
    daily_by_token['identifier'] = daily_by_token['date'].apply(str) + '_' + daily_by_token['token']
    
    return daily_by_token.round(2)

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
        'avg_daily_volume': df['amount_usd'].sum() / days_since_launch,

        # ID column for BigQuery
        'updated_on': date.today()
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
            'total_deposits_usd': deposits['amount_usd'].sum(),
            'total_withdrawals_usd': withdrawals['amount_usd'].sum(),
            'net_flow': deposits['amount_usd'].sum() - withdrawals['amount_usd'].sum(),
            'total_volume': token_data['amount_usd'].sum(),
            # 'tvl': df.groupby(level='token')['net_flow'].cumsum(),

            
            # Amount metrics (native units)
            'total_deposits_native': deposits['amount'].sum(),
            'total_withdrawals_native': withdrawals['amount'].sum(),
            'net_flow_native': deposits['amount'].sum() - withdrawals['amount'].sum(),
            
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

# USER METRICS
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
    
    # Remove users with '0' value
    user_metrics.drop([0,0])

    # Reset index to create identifier column
    user_metrics = user_metrics.reset_index()
    user_metrics['user'] = user_metrics['user'].apply(str)
    
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

# HEALTH INDICATORS
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
        'outflow_ratio_7d': daily_df['withdrawal_amount_usd'].iloc[-7:].sum() / 
                            daily_df['deposit_amount_usd'].iloc[-7:].sum() if daily_df['deposit_amount_usd'].iloc[-7:].sum() > 0 else 0,
        
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

    health['updated_on'] = date.today()
    
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

    # Calculate previous period metrics for % change
    prev_timeframes = {
        '24h': df[df['date'] == current_date - timedelta(days=1)],
        '7d': df[(df['date'] >= current_date - timedelta(days=14)) & (df['date'] < current_date - timedelta(days=7))],
        '30d': df[(df['date'] >= current_date - timedelta(days=60)) & (df['date'] < current_date - timedelta(days=30))]
    }

    # Print deposit and withdrawal metrics
    print("\n📥 DEPOSITS:")
    print("-" * 80)
    print(f"{'Period':<12} {'Count':>10} {'Amount':>20} {'% Change':>15}")
    print("-" * 80)

    for period_name, period_data in timeframes.items():
        deposits = period_data[period_data['type'] == 'deposit']
        deposit_count = len(deposits)
        deposit_amount_usd = deposits['amount_usd'].sum()

        # Calculate % change (skip for All-time)
        pct_change_str = ""
        if period_name in prev_timeframes:
            prev_deposits = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'deposit']
            prev_amount = prev_deposits['amount_usd'].sum()
            if prev_amount > 0:
                pct_change = ((deposit_amount_usd - prev_amount) / prev_amount) * 100
                pct_change_str = f"{pct_change:+.1f}%"
            else:
                pct_change_str = "N/A"

        print(f"{period_name:<12} {deposit_count:>10,} {'$':>15}{deposit_amount_usd:>1,.0f} {pct_change_str:>15}")
    
    print("\n📤 WITHDRAWALS:")
    print("-" * 80)
    print(f"{'Period':<12} {'Count':>10} {'Amount':>20} {'% Change':>15}")
    print("-" * 80)

    for period_name, period_data in timeframes.items():
        withdrawals = period_data[period_data['type'] == 'withdrawal']
        withdrawal_count = len(withdrawals)
        withdrawal_amount_usd = withdrawals['amount_usd'].sum()

        # Calculate % change (skip for All-time)
        pct_change_str = ""
        if period_name in prev_timeframes:
            prev_withdrawals = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'withdrawal']
            prev_amount = prev_withdrawals['amount_usd'].sum()
            if prev_amount > 0:
                pct_change = ((withdrawal_amount_usd - prev_amount) / prev_amount) * 100
                pct_change_str = f"{pct_change:+.1f}%"
            else:
                pct_change_str = "N/A"

        print(f"{period_name:<12} {withdrawal_count:>10,} {'$':>15}{withdrawal_amount_usd:>1,.0f} {pct_change_str:>15}")
    
    print("\n💱 NET FLOW:")
    print("-" * 80)
    print(f"{'Period':<12} {'Net Count':>10} {'Net Amount':>20} {'% Change':>15}")
    print("-" * 80)

    for period_name, period_data in timeframes.items():
        deposits = period_data[period_data['type'] == 'deposit']
        withdrawals = period_data[period_data['type'] == 'withdrawal']
        net_count = len(deposits) - len(withdrawals)
        net_amount = deposits['amount_usd'].sum() - withdrawals['amount_usd'].sum()

        # Calculate % change (skip for All-time)
        pct_change_str = ""
        if period_name in prev_timeframes:
            prev_deposits = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'deposit']
            prev_withdrawals = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'withdrawal']
            prev_net_amount = prev_deposits['amount_usd'].sum() - prev_withdrawals['amount_usd'].sum()
            if abs(prev_net_amount) > 0:
                pct_change = ((net_amount - prev_net_amount) / abs(prev_net_amount)) * 100
                pct_change_str = f"{pct_change:+.1f}%"
            else:
                pct_change_str = "N/A"

        print(f"{period_name:<12} {net_count:>10,} {'$':>11}{net_amount:>1,.0f} {pct_change_str:>15}")
    
    # Top tokens
    print("\n🪙 TOP TOKENS BY VOLUME:")
    print("-" * 50)
    top_tokens = metrics['summary_by_token'].head(5)[['token', 'total_volume', 'volume_share_pct', 'net_flow']]
    for _, row in top_tokens.iterrows():
        print(f"  {row['token']:<8} {'$':>4}{row['total_volume']:>1,.0f} - {row['volume_share_pct']:.1f}{'%':>2} {'|':>2} {'Net: $':<2}{row['net_flow']:,.0f}")
    
    # User metrics with % change
    print("\n👥 USER ACTIVITY:")
    print("-" * 80)
    print(f"  {'Period':<20} {'Active Users':>15} {'% Change':>15}")
    print("-" * 80)

    # Calculate user metrics for each period
    user_metrics = {
        '24h': (summary['unique_users_24h'],
                df[df['date'] == current_date - timedelta(days=1)]['depositor'].nunique() +
                df[df['date'] == current_date - timedelta(days=1)]['withdrawer'].nunique()),
        '7d': (summary['unique_users_7d'],
               df[(df['date'] >= current_date - timedelta(days=14)) &
                  (df['date'] < current_date - timedelta(days=7))].apply(
                      lambda x: x['depositor'] if pd.notna(x.get('depositor')) else x.get('withdrawer'), axis=1).nunique()),
        '30d': (summary['unique_users_30d'],
                df[(df['date'] >= current_date - timedelta(days=60)) &
                   (df['date'] < current_date - timedelta(days=30))].apply(
                       lambda x: x['depositor'] if pd.notna(x.get('depositor')) else x.get('withdrawer'), axis=1).nunique())
    }

    print(f"  {'24h Active Users':<20} {summary['unique_users_24h']:>15,.0f} {f'{((summary["unique_users_24h"] - user_metrics["24h"][1]) / user_metrics["24h"][1] * 100):+.1f}%' if user_metrics['24h'][1] > 0 else 'N/A':>15}")
    print(f"  {'7d Active Users':<20} {summary['unique_users_7d']:>15,.0f} {f'{((summary["unique_users_7d"] - user_metrics["7d"][1]) / user_metrics["7d"][1] * 100):+.1f}%' if user_metrics['7d'][1] > 0 else 'N/A':>15}")
    print(f"  {'30d Active Users':<20} {summary['unique_users_30d']:>15,.0f} {f'{((summary["unique_users_30d"] - user_metrics["30d"][1]) / user_metrics["30d"][1] * 100):+.1f}%' if user_metrics['30d'][1] > 0 else 'N/A':>15}")
    print(f"  {'All-time Users':<20} {summary['total_unique_users_all_time']:>15,.0f} {'':<15}")
    
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
# RUN MAIN PROCESS
# ==================================================
        
def main(skip_bigquery=False, sample_size=False, test_mode=False):
    """Main function to process bridge transaction data."""
    ProgressIndicators.print_header("BRIDGE DATA PROCESSING PIPELINE")

    conversions = Conversions()

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        if sample_size:
            print(f"{'Using sample size: ' + str(sample_size):^60}")
        if skip_bigquery:
            print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    try:
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.5f}'.format
        # path = '/Users/laurenjackson/Desktop/mezo-analytics/tests'

        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data') 
        # change project ID to 'mezo-data-dev' when testing

        ProgressIndicators.print_step("Environment loaded successfully", "success")
        
    # ==================================================
    # GET RAW BRIDGE DATA
    # ==================================================
    
        if not test_mode:

            ProgressIndicators.print_step("Fetching raw bridge deposit data", "start")
            raw_deposits = SubgraphClient.get_subgraph_data(
                SubgraphClient.MEZO_BRIDGE_SUBGRAPH,
                BridgeQueries.GET_BRIDGE_TRANSACTIONS,
                'assetsLockeds'
            )
            ProgressIndicators.print_step(f"Retrieved {len(raw_deposits) if raw_deposits is not None else 0} deposit transactions", "success")

            ProgressIndicators.print_step("Fetching raw bridge withdrawal data", "start")
            raw_withdrawals = SubgraphClient.get_subgraph_data(
                SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH,
                BridgeQueries.GET_NATIVE_WITHDRAWALS,
                'assetsUnlockeds'
            )
            ProgressIndicators.print_step(f"Retrieved {len(raw_withdrawals) if raw_withdrawals is not None else 0} withdrawal transactions", "success")

            ProgressIndicators.print_step("Saving CSVs for test mode", "start")
            raw_deposits.to_csv(f'raw_deposits.csv')
            raw_withdrawals.to_csv(f'raw_withdrawals.csv')
            ProgressIndicators.print_step(f"Retrieved {len(raw_withdrawals) if raw_withdrawals is not None else 0} withdrawal transactions", "success")        
        
        else:
            
            raw_deposits = pd.read_csv(f'raw_deposits.csv')
            raw_withdrawals = pd.read_csv(f'raw_withdrawals.csv')

    # ==========================================================
    # UPLOAD RAW DATA TO BIGQUERY
    # ==========================================================
        
        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
            raw_datasets = [
                (raw_deposits, 'bridge_transactions_raw', 'transactionHash_'),
                (raw_withdrawals, 'bridge_withdrawals_raw', 'transactionHash_'),
            ]

            for dataset, table_name, id_column in raw_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'raw_data', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")
                    # ProgressIndicators.print_step("Saving CSVs for test mode", "start")
                    # dataset.to_csv(f'{table_name}.csv')
                    # ProgressIndicators.print_step("CSVs saved", "success")

    # ==================================================
    # CLEAN BRIDGE DEPOSIT + WITHDRAWAL DATA
    # ==================================================
        ProgressIndicators.print_step("Processing deposits data", "start")
        deposits = clean_bridge_data(
            raw=raw_deposits, sort_col='timestamp_',
            date_cols=['timestamp_'], currency_cols=['amount'], 
            asset_col='token', txn_type='deposit'
        )
        deposits_with_usd = conversions.add_usd_conversions(
            deposits, token_column='token', amount_columns =['amount']
        )
        deposits_clean = deposits_with_usd[[
            'timestamp_', 'amount', 'token', 'amount_usd',
            'recipient', 'transactionHash_', 'type']]
        deposits_clean = deposits_clean.rename(columns={'recipient': 'depositor'})
        ProgressIndicators.print_step(f"Processed {len(deposits_clean)} deposit records", "success")

        # clean withdrawals data
        ProgressIndicators.print_step("Processing withdrawals data", "start")
        withdrawals = clean_bridge_data(
            raw_withdrawals, sort_col='timestamp_',
            date_cols=['timestamp_'], currency_cols=['amount'], 
            asset_col='token', txn_type='withdrawal'
        )
        withdrawals_with_usd = conversions.add_usd_conversions(
            withdrawals, token_column='token', amount_columns=['amount']
        )
        bridge_map = {'0': 'ethereum', '1': 'bitcoin'}
        withdrawals_with_usd['chain'] = withdrawals_with_usd['chain'].map(bridge_map)
        withdrawals_clean = withdrawals_with_usd[[
            'timestamp_', 'amount', 'token', 'amount_usd', 'chain',
            'recipient', 'sender', 'transactionHash_', 'type']]
        withdrawals_clean = withdrawals_clean.rename(columns={'sender': 'withdrawer', 'recipient': 'withdraw_recipient'})
        ProgressIndicators.print_step(f"Processed {len(withdrawals_clean)} withdrawal records", "success")

    # ==========================================================
    # UPLOAD CLEAN DEPOSIT + WITHDRAWAL TABLES TO BIGQUERY
    # ==========================================================
        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
            clean_datasets = [
                (deposits_clean, 'bridge_deposits_clean', 'transactionHash_'),
                (withdrawals_clean, 'bridge_withdrawals_clean', 'transactionHash_'),
            ]

            for dataset, table_name, id_column in clean_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'staging', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")


    # ==================================================
    # ADD BRIDGE VOLUME AND BRIDGE TVL TO TABLE
    # ==================================================

        # Combine the deposit and withdrawal data tables
        ProgressIndicators.print_step("Combining deposit and withdrawal data", "start")
        combined = pd.concat(
            [deposits_clean, withdrawals_clean], ignore_index=True
        ).fillna(0)
        combined = combined.sort_values('timestamp_').reset_index(drop=True)
        ProgressIndicators.print_step(f"Combined {len(combined)} total bridge transactions", "success")

        # Calculate net flow, tvl, volume
        ProgressIndicators.print_step("Calculating net flow, TVL, and volume", "start")        
        combined['net_flow'] = np.where(
            combined['type'] == 'deposit',
            combined['amount_usd'],
            -combined['amount_usd']
        )
        combined['deposit_amount_usd'] = np.where(
            combined['type'] == 'deposit',
            combined['amount_usd'], 0
        )
        combined['withdrawal_amount_usd'] = np.where(
            combined['type'] == 'withdrawal',
            combined['amount_usd'], 0
        )
        combined['volume'] = combined['withdrawal_amount_usd'] + combined['deposit_amount_usd']
        combined['tvl'] = combined['net_flow'].cumsum() 
        ProgressIndicators.print_step("Calculations complete", "success")

    # ==================================================
    # AGGREGATE TVL AND NET FLOW BY DAY
    # ==================================================
        
        ProgressIndicators.print_step("Aggregating TVL data by day", "start")
        tvl = combined.copy()

        daily_tvl = tvl.groupby(['timestamp_']).agg(
                # TVL metrics (end of day values)
                tvl = ('tvl', 'last'),
                net_flow = ('net_flow', 'sum'),  # Net daily flow
                deposits_usd = ('deposit_amount_usd', 'sum'),  # Total daily deposits
                withdrawals_usd = ('withdrawal_amount_usd', 'sum'),  # Total daily withdrawals
                tx_type = ('type', 'count'),  # Total transactions
            ).reset_index()

        # Calculate deposit and withdrawal counts
        deposit_counts = tvl[tvl['type'] == 'deposit'].groupby(['timestamp_']).size()
        depositors = tvl[tvl['type'] == 'deposit'].groupby(['timestamp_'])['depositor'].nunique()
        withdrawal_counts = tvl[tvl['type'] == 'withdrawal'].groupby(['timestamp_']).size()
        withdrawers = tvl[tvl['type'] == 'withdrawal'].groupby(['timestamp_'])['withdrawer'].nunique()
            
        # Add transaction counts to daily metrics
        daily_tvl = daily_tvl.set_index(['timestamp_'])
        daily_tvl['deposits'] = deposit_counts
        daily_tvl['depositors'] = depositors
        daily_tvl['withdrawals'] = withdrawal_counts
        daily_tvl['withdrawers'] = withdrawers
        daily_tvl['unique_wallets'] = daily_tvl['withdrawers'] + daily_tvl['depositors']
        daily_tvl['total_transactions'] = daily_tvl['deposits'] + daily_tvl['withdrawals']
        daily_tvl = daily_tvl.fillna(0).reset_index()

        # Calculate protocol-wide additional metrics
        daily_tvl['deposit_withdrawal_ratio'] = np.where(
            daily_tvl['withdrawals'] > 0,
            daily_tvl['deposits'] / daily_tvl['withdrawals'],
            np.inf
        )
            
        # TVL - no moving average, but track changes
        daily_tvl['tvl_change'] = daily_tvl['tvl'].diff()
        daily_tvl['tvl_change_pct'] = daily_tvl['tvl'].pct_change()
        daily_tvl['tvl_ath'] = daily_tvl['tvl'].cummax()

        daily_tvl['drawdown_from_ath'] = (
            (daily_tvl['tvl'] - daily_tvl['tvl_ath']) / 
            daily_tvl['tvl_ath']
        )
        for col in ['deposits', 'withdrawals', 'net_flow']:
            daily_tvl[f'{col}_ma7'] = daily_tvl[col].rolling(window=7).mean()
            daily_tvl[f'{col}_ma30'] = daily_tvl[col].rolling(window=7).mean()

        daily_tvl = daily_tvl.fillna(0)
        ProgressIndicators.print_step("Aggregation complete", "success")

    # ========================================
    # AGGREGATE VOLUME BY DAY
    # ========================================
        
        ProgressIndicators.print_step("Aggregating volume data by day", "start")
        daily_volume = daily_tvl.copy()
        daily_volume['volume'] = daily_volume['withdrawals_usd'] + daily_volume['deposits_usd']
        daily_volume['volume_7d_ma'] = daily_volume['volume'].rolling(window=7).mean()
        daily_volume['volume_30d_ma'] = daily_volume['volume'].rolling(window=30).mean()
        daily_volume['volume_change'] = daily_volume['volume'].pct_change()
        daily_volume['volume_change_7d'] = daily_volume['volume_7d_ma'].pct_change()
        daily_volume['volume_change_30d'] = daily_volume['volume_30d_ma'].pct_change()
        daily_volume['is_significant_volume'] = daily_volume['volume'].transform(lambda x: x > x.quantile(0.9))
        daily_volume = daily_volume.fillna(0)
        ProgressIndicators.print_step("Aggregation complete", "success")

    # ========================================
    # UPLOAD VOLUME BY DAY TO BIGQUERY
    # ========================================
        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading daily data to BigQuery", "start")
            daily_datasets = [
                (daily_tvl, 'agg_bridge_daily-tvl', 'timestamp_'),
                (daily_volume, 'agg_bridge_daily-volume', 'timestamp_'),
            ]

            for dataset, table_name, id_column in daily_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'marts', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

    # ========================================
    # RUN COMPREHENSIVE BRIDGE METRICS
    # ========================================

        ProgressIndicators.print_step("Create comprehensive bridge metrics dataframes...", "start")
        metrics = calculate_bridge_metrics(combined)
        ProgressIndicators.print_step(f"Generated comprehensive metrics tables.", "success")

        # Access intermediate dataframes
        user_metrics = metrics['user_metrics']
        daily_by_token = metrics['daily_by_token']

        # Access clean/final dataframes
        daily_overall = metrics['daily_overall']
        summary_overall = metrics['summary_overall']
        summary_by_token = metrics['summary_by_token']
        health_metrics = metrics['health_metrics']
        user_metrics_by_token = metrics['user_metrics_by_token']

    # ==========================================================
    # UPLOAD AGGREGATED DATA TO BIGQUERY
    # ==========================================================
        
        if not skip_bigquery:
            # INTERMEDIATE TABLES
            ProgressIndicators.print_step("Uploading intermediate tables to BigQuery", "start")
            int_datasets = [
                (user_metrics, 'int_bridge_user-metrics', 'user'),
                (daily_by_token, 'int_bridge_daily-by-token', 'identifier')
            ]

            for dataset, table_name, id_column in int_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'intermediate', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

            # MARTS TABLES
            ProgressIndicators.print_step("Uploading marts tables to BigQuery", "start")
            marts_datasets = [
                (daily_overall, 'marts_bridge_daily-overall', 'date'),
                (user_metrics_by_token, 'marts_bridge_user-metrics-by-token', 'token'),
                (summary_by_token, 'marts_bridge_summary-by-token', 'token')
            ]

            for dataset, table_name, id_column in marts_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'marts', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")
            
            ProgressIndicators.print_step("Uploading marts tables to BigQuery", "start")
            upsert_datasets = [
                (summary_overall, 'marts_bridge_summary', 'updated_on'),
                (health_metrics, 'marts_bridge_health-metrics', 'updated_on')
            ]

            for dataset, table_name, id_column in upsert_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.upsert_table_by_id(dataset, 'marts', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # Display summary
        display_summary(metrics, combined)

        # Generate and save markdown report
        ProgressIndicators.print_step("Generating markdown report", "start")
        generator = ReportGenerator()
        bridge_report = generator.generate_bridge_report(metrics, combined)

        # Save report to file
        report_filename = f"bridge_report_{datetime.now().strftime('%Y%m%d')}.md"
        report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', report_filename)
        with open(report_path, 'w') as f:
            f.write(bridge_report)
        ProgressIndicators.print_step(f"Saved markdown report to {report_filename}", "success")

        ProgressIndicators.print_header(f"{ProgressIndicators.SUCCESS} BRIDGE DATA PROCESSING COMPLETE")
    
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"{ProgressIndicators.ERROR} PROCESSING FAILED")
        print(f"\n{ProgressIndicators.INFO} Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise

if __name__ == "__main__":
    results = main()