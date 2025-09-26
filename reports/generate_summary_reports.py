#!/usr/bin/env python3
"""
Generate Summary Reports for All Data Processing Scripts
Exports key metrics to markdown files for daily reporting
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any
from io import StringIO
import sys
import os

try:
    from linear_api import LinearClient
    LINEAR_AVAILABLE = True
except ImportError:
    LINEAR_AVAILABLE = False
    print("⚠️  linear-api package not installed. Linear integration disabled.")

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def display_bridge_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of bridge metrics (moved from process_bridge_data.py)"""

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

def capture_output(func, *args, **kwargs):
    """Capture stdout from a function and return as string"""
    old_stdout = sys.stdout
    sys.stdout = captured_output = StringIO()
    try:
        func(*args, **kwargs)
        return captured_output.getvalue()
    finally:
        sys.stdout = old_stdout

def export_to_markdown(content: str, filename: str, title: str):
    """Export content to markdown file with header"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    with open(filename, 'w') as f:
        f.write(f"# {title}\n\n")
        f.write(f"*Generated on {timestamp}*\n\n")
        f.write("```\n")
        f.write(content)
        f.write("\n```\n")

def create_linear_document(title: str, content: str, team_id: str = None):
    """Create a Linear document from markdown content"""
    if not LINEAR_AVAILABLE:
        print("❌ Linear API not available. Skipping Linear document creation.")
        return None

    try:
        # Get API key from environment
        api_key = os.getenv('LINEAR_API_KEY')
        if not api_key:
            print("❌ LINEAR_API_KEY environment variable not set.")
            return None

        # Initialize Linear client
        client = LinearClient(api_key=api_key)

        # Get current user and default team if not specified
        if not team_id:
            me = client.users.get_me()
            teams = client.teams.list()
            if teams:
                team_id = teams[0].id
                print(f"📝 Using default team: {teams[0].name}")
            else:
                print("❌ No teams found for Linear document creation.")
                return None

        # Create issue (Linear doesn't have direct document creation, so we use issues)
        # The content will be in the description
        timestamp = datetime.now().strftime("%Y-%m-%d")
        issue_title = f"Daily Report: {title} - {timestamp}"

        issue_data = {
            "title": issue_title,
            "description": content,
            "teamId": team_id,
            "priority": 2,  # Medium priority
            "labels": ["daily-report", "automated"]
        }

        # Create the issue
        new_issue = client.issues.create(issue_data)
        print(f"✅ Created Linear issue: {new_issue.identifier} - {issue_title}")

        return new_issue

    except Exception as e:
        print(f"❌ Error creating Linear document: {str(e)}")
        return None

def create_all_linear_documents(reports_data: Dict[str, str]):
    """Create Linear documents for all generated reports"""
    if not LINEAR_AVAILABLE:
        return

    print("\n📝 Creating Linear documents...")

    for report_name, data in reports_data.items():
        title = data['title']
        content = data['content']

        linear_content = f"# {title}\n\n{content}"
        result = create_linear_document(title, linear_content)

        if result:
            print(f"✅ {report_name} report added to Linear")
        else:
            print(f"❌ Failed to create Linear document for {report_name}")

    print("📝 Linear document creation completed")

def display_musd_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of MUSD metrics"""

    print("\n" + "="*60)
    print("💰 MUSD METRICS SUMMARY")
    print("="*60)

    current_date = df['date'].max() if 'date' in df.columns else datetime.now().date()

    # Define timeframes
    timeframes = {
        '24h': df[df['date'] == current_date] if 'date' in df.columns else df.tail(1),
        '7d': df[df['date'] >= current_date - timedelta(days=7)] if 'date' in df.columns else df.tail(7),
        '30d': df[df['date'] >= current_date - timedelta(days=30)] if 'date' in df.columns else df.tail(30),
        'All-time': df
    }

    # MUSD supply metrics
    if 'total_supply' in df.columns:
        current_supply = df['total_supply'].iloc[-1] if len(df) > 0 else 0
        print(f"\n💵 Total MUSD Supply: {current_supply:,.0f}")

        # Supply changes
        print("\n📈 SUPPLY CHANGES:")
        print("-" * 50)
        print(f"{'Period':<12} {'Supply Change':>20}")
        print("-" * 50)

        for period_name, period_data in timeframes.items():
            if len(period_data) > 1:
                supply_change = period_data['total_supply'].iloc[-1] - period_data['total_supply'].iloc[0]
                sign = "+" if supply_change > 0 else ""
                print(f"{period_name:<12} {sign}{supply_change:>18,.0f}")

    # Transaction metrics
    if 'transaction_count' in df.columns:
        print("\n📊 TRANSACTION ACTIVITY:")
        print("-" * 50)
        print(f"{'Period':<12} {'Transactions':>15}")
        print("-" * 50)

        for period_name, period_data in timeframes.items():
            tx_count = period_data['transaction_count'].sum() if len(period_data) > 0 else 0
            print(f"{period_name:<12} {tx_count:>15,}")

    # Top holders if available
    if 'summary_by_holder' in metrics:
        print("\n🏦 TOP HOLDERS:")
        print("-" * 50)
        top_holders = metrics['summary_by_holder'].head(5)
        for _, row in top_holders.iterrows():
            if 'balance' in row and 'address' in row:
                print(f"  {row['address'][:10]}... {row['balance']:>15,.0f} MUSD")

def display_market_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of market data metrics"""

    print("\n" + "="*60)
    print("📈 MARKET DATA SUMMARY")
    print("="*60)

    current_date = df['date'].max() if 'date' in df.columns else datetime.now().date()

    # Price metrics
    if 'price_usd' in df.columns:
        current_price = df['price_usd'].iloc[-1] if len(df) > 0 else 0
        print(f"\n💲 Current Price: ${current_price:,.4f}")

        # Price changes
        if len(df) >= 7:
            price_7d_ago = df['price_usd'].iloc[-7]
            price_change_7d = ((current_price - price_7d_ago) / price_7d_ago) * 100
            print(f"📊 7d Change: {price_change_7d:+.2f}%")

        if len(df) >= 30:
            price_30d_ago = df['price_usd'].iloc[-30]
            price_change_30d = ((current_price - price_30d_ago) / price_30d_ago) * 100
            print(f"📊 30d Change: {price_change_30d:+.2f}%")

    # Volume metrics
    if 'volume_24h' in df.columns:
        print("\n📊 TRADING VOLUME:")
        print("-" * 50)

        timeframes = {
            '24h': df.tail(1),
            '7d': df.tail(7),
            '30d': df.tail(30),
        }

        for period_name, period_data in timeframes.items():
            volume = period_data['volume_24h'].sum() if len(period_data) > 0 else 0
            print(f"{period_name:<12} ${volume:>18,.0f}")

    # Market cap if available
    if 'market_cap' in df.columns:
        current_mcap = df['market_cap'].iloc[-1] if len(df) > 0 else 0
        print(f"\n🏛️ Market Cap: ${current_mcap:,.0f}")

def display_pools_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of pools metrics"""

    print("\n" + "="*60)
    print("🏊 POOLS METRICS SUMMARY")
    print("="*60)

    # TVL metrics
    if 'tvl_usd' in df.columns:
        current_tvl = df['tvl_usd'].sum() if len(df) > 0 else 0
        print(f"\n💰 Total TVL: ${current_tvl:,.0f}")

    # Pool count
    if 'pool_address' in df.columns:
        unique_pools = df['pool_address'].nunique()
        print(f"🏊 Active Pools: {unique_pools:,}")

    # Top pools by TVL
    if 'summary_by_pool' in metrics:
        print("\n🏆 TOP POOLS BY TVL:")
        print("-" * 50)
        top_pools = metrics['summary_by_pool'].head(5)
        for _, row in top_pools.iterrows():
            if 'tvl_usd' in row and 'pool_name' in row:
                print(f"  {row['pool_name']:<20} ${row['tvl_usd']:>15,.0f}")

    # Volume metrics
    if 'volume_24h' in df.columns:
        total_volume_24h = df['volume_24h'].sum() if len(df) > 0 else 0
        print(f"\n📊 24h Volume: ${total_volume_24h:,.0f}")

def display_swaps_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of swaps metrics"""

    print("\n" + "="*60)
    print("🔄 SWAPS METRICS SUMMARY")
    print("="*60)

    current_date = df['date'].max() if 'date' in df.columns else datetime.now().date()

    # Define timeframes
    timeframes = {
        '24h': df[df['date'] == current_date] if 'date' in df.columns else df.tail(1),
        '7d': df[df['date'] >= current_date - timedelta(days=7)] if 'date' in df.columns else df.tail(7),
        '30d': df[df['date'] >= current_date - timedelta(days=30)] if 'date' in df.columns else df.tail(30),
        'All-time': df
    }

    # Swap volume metrics
    print("\n💱 SWAP VOLUME:")
    print("-" * 50)
    print(f"{'Period':<12} {'Count':>10} {'Volume (USD)':>20}")
    print("-" * 50)

    for period_name, period_data in timeframes.items():
        swap_count = len(period_data)
        swap_volume = period_data['amount_usd'].sum() if 'amount_usd' in period_data.columns and len(period_data) > 0 else 0
        print(f"{period_name:<12} {swap_count:>10,} ${swap_volume:>18,.0f}")

    # Top token pairs
    if 'summary_by_pair' in metrics:
        print("\n🔄 TOP TRADING PAIRS:")
        print("-" * 50)
        top_pairs = metrics['summary_by_pair'].head(5)
        for _, row in top_pairs.iterrows():
            if 'pair' in row and 'volume_usd' in row:
                print(f"  {row['pair']:<20} ${row['volume_usd']:>15,.0f}")

    # Unique users
    if 'user_address' in df.columns:
        unique_users = df['user_address'].nunique()
        print(f"\n👥 Unique Swappers: {unique_users:,}")

def display_vaults_summary(metrics: Dict[str, pd.DataFrame], df: pd.DataFrame):
    """Display formatted summary of vaults metrics"""

    print("\n" + "="*60)
    print("🏦 VAULTS METRICS SUMMARY")
    print("="*60)

    # Total value locked
    if 'tvl_usd' in df.columns:
        current_tvl = df['tvl_usd'].sum() if len(df) > 0 else 0
        print(f"\n💰 Total TVL: ${current_tvl:,.0f}")

    # Vault count
    if 'vault_address' in df.columns:
        unique_vaults = df['vault_address'].nunique()
        print(f"🏦 Active Vaults: {unique_vaults:,}")

    # Top vaults by TVL
    if 'summary_by_vault' in metrics:
        print("\n🏆 TOP VAULTS BY TVL:")
        print("-" * 50)
        top_vaults = metrics['summary_by_vault'].head(5)
        for _, row in top_vaults.iterrows():
            if 'tvl_usd' in row and 'vault_name' in row:
                print(f"  {row['vault_name']:<20} ${row['tvl_usd']:>15,.0f}")

    # Yield metrics
    if 'apy' in df.columns:
        avg_apy = df['apy'].mean() if len(df) > 0 else 0
        print(f"\n📈 Average APY: {avg_apy:.2f}%")

    # Deposit/withdrawal activity
    if 'deposits_24h' in df.columns and 'withdrawals_24h' in df.columns:
        total_deposits = df['deposits_24h'].sum() if len(df) > 0 else 0
        total_withdrawals = df['withdrawals_24h'].sum() if len(df) > 0 else 0
        net_flow = total_deposits - total_withdrawals

        print(f"\n💰 24h Activity:")
        print(f"  Deposits: ${total_deposits:,.0f}")
        print(f"  Withdrawals: ${total_withdrawals:,.0f}")
        print(f"  Net Flow: ${net_flow:+,.0f}")

def generate_all_summaries():
    """Generate summary reports for all data processing scripts"""

    print("🚀 Generating summary reports for all data sources...")

    # This is a template - in practice, you would import the actual data
    # and metrics from each processing script

    # Example data structures (replace with actual data loading)
    empty_df = pd.DataFrame()
    empty_metrics = {}

    reports = {
        'bridge': {
            'function': display_bridge_summary,
            'title': 'Bridge Data Summary',
            'filename': f'bridge_summary_{datetime.now().strftime("%Y%m%d")}.md'
        },
        'musd': {
            'function': display_musd_summary,
            'title': 'MUSD Data Summary',
            'filename': f'musd_summary_{datetime.now().strftime("%Y%m%d")}.md'
        },
        'market': {
            'function': display_market_summary,
            'title': 'Market Data Summary',
            'filename': f'market_summary_{datetime.now().strftime("%Y%m%d")}.md'
        },
        'pools': {
            'function': display_pools_summary,
            'title': 'Pools Data Summary',
            'filename': f'pools_summary_{datetime.now().strftime("%Y%m%d")}.md'
        },
        'swaps': {
            'function': display_swaps_summary,
            'title': 'Swaps Data Summary',
            'filename': f'swaps_summary_{datetime.now().strftime("%Y%m%d")}.md'
        },
        'vaults': {
            'function': display_vaults_summary,
            'title': 'Vaults Data Summary',
            'filename': f'vaults_summary_{datetime.now().strftime("%Y%m%d")}.md'
        }
    }

    # Store report data for Linear integration
    reports_data = {}

    for report_name, config in reports.items():
        try:
            print(f"\n📊 Generating {report_name} summary...")

            # Capture the output
            content = capture_output(config['function'], empty_metrics, empty_df)

            # Export to markdown
            export_to_markdown(content, config['filename'], config['title'])

            # Store for Linear integration
            reports_data[report_name] = {
                'title': config['title'],
                'content': content,
                'filename': config['filename']
            }

            print(f"✅ {config['filename']} generated successfully")

        except Exception as e:
            print(f"❌ Error generating {report_name} summary: {str(e)}")

    # Create Linear documents if enabled
    if LINEAR_AVAILABLE and reports_data:
        create_all_linear_documents(reports_data)

    return reports_data

if __name__ == "__main__":
    generate_all_summaries()