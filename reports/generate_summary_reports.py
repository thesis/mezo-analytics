#!/usr/bin/env python3
"""
Generate summary reports from processing scripts and upload to Linear.

This script:
1. Imports and runs data processing scripts to get real metrics
2. Generates formatted markdown reports 
3. Uploads reports to Linear docs via GraphQL API
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import processing scripts that generate summaries
from scripts.archive.pools_v3 import main as process_pools

# set the linear project id for the data reports project
LINEAR_PROJECT_ID='8cf7b30b-0031-4f26-b3ad-59828750fce3'
LINEAR_DOC_ID = None
LINEAR_SUMMARY_DOC_ID = 'd35f442d-73f8-4462-a1fb-f8e99704ae96'

# ==================================================
# LINEAR API CLIENT
# ==================================================

class LinearAPIClient:
    """Client for interacting with Linear GraphQL API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.linear.app/graphql"
        self.headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }

    def execute_query(self, query: str, variables: dict = None):
        """Execute a GraphQL query against Linear API."""
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        response = requests.post(
            self.base_url,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"Linear API error: {response.status_code} - {response.text}")
        
        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        return data.get("data", {})
    
    def create_document(self, title: str, content: str, project_id: Optional[str] = None):
        """Create a new document in Linear."""
        query = """
        mutation CreateDocument($title: String!, $content: String!, $projectId: String) {
            documentCreate(input: { title: $title, content: $content, projectId: $projectId }) {
                success
                document {
                    id
                    title
                    url
                }
            }
        }
        """
        
        variables = {
            "title": title,
            "content": content,
            "projectId": project_id
        }
        
        result = self.execute_query(query, variables)
        doc_data = result.get("documentCreate", {})
        
        if not doc_data.get("success"):
            raise Exception("Failed to create Linear document")
        
        return doc_data.get("document", {})
    
    def update_document(self, document_id: str, title: str = None, content: str = None):
        """Update an existing Linear document."""
        query = """
        mutation UpdateDocument($id: String!, $title: String, $content: String) {
            documentUpdate(id: $id, input: { title: $title, content: $content }) {
                success
                document {
                    id
                    title
                    url
                    updatedAt
                }
            }
        }
        """
        
        variables = {"id": document_id}
        if title:
            variables["title"] = title
        if content:
            variables["content"] = content
        
        result = self.execute_query(query, variables)
        return result.get("documentUpdate", {})
    
    def get_document(self, document_id: str):
        """Get a document by ID."""
        query = """
        query GetDocument($id: String!) {
            document(id: $id) {
                id
                title
                content
                url
                createdAt
                updatedAt
            }
        }
        """
        
        result = self.execute_query(query, {"id": document_id})
        return result.get("document", {})

# ==================================================
# REPORT GENERATORS
# ==================================================

class ReportGenerator:
    """Generate markdown reports from processing script outputs."""
    
    @staticmethod
    def format_number(value: float, decimals: int = 2):
        """Format number with commas and decimals."""
        if pd.isna(value) or value is None:
            return "N/A"
        if value >= 1_000_000:
            return f"${value/1_000_000:,.{decimals}f}M"
        elif value >= 1_000:
            return f"${value/1_000:,.{decimals}f}K"
        else:
            return f"${value:,.{decimals}f}"
    
    @staticmethod
    def format_percentage(value: float, decimals: int = 2):
        """Format percentage value."""
        if pd.isna(value) or value is None:
            return "N/A"
        return f"{value:.{decimals}f}%"
    
    def generate_pools_report(self, pools_data: Dict[str, Any]):
        """Generate markdown report for pools data."""
        
        # Extract data from pools processing
        tvl_snapshot = pools_data.get('tvl_snapshot')
        efficiency_metrics = pools_data.get('efficiency_metrics')
        total_tvl = pools_data.get('total_tvl', 0)
        active_pools = pools_data.get('active_pools', 0)
        
        # Get latest daily metrics if available
        daily_pool_tvl = pools_data.get('daily_pool_tvl')
        daily_protocol_tvl = pools_data.get('daily_protocol_tvl')
        
        # Calculate 7-day metrics
        seven_day_tvl_change = 0
        seven_day_volume = 0
        if daily_protocol_tvl is not None and len(daily_protocol_tvl) > 7:
            current_tvl = daily_protocol_tvl.iloc[-1]['protocol_tvl_total']
            week_ago_tvl = daily_protocol_tvl.iloc[-8]['protocol_tvl_total']
            if week_ago_tvl > 0:
                seven_day_tvl_change = ((current_tvl - week_ago_tvl) / week_ago_tvl) * 100
        
        # Build markdown report
        report = f"""# 🏊 Liquidity Pools Analytics Report
*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}*

---

## 📊 Executive Summary

### Key Metrics
| Metric | Value | 7D Change |
|--------|-------|-----------|
| **Total TVL** | {self.format_number(total_tvl)} | {self.format_percentage(seven_day_tvl_change)} |
| **Active Pools** | {active_pools} | - |
| **Avg Pool Efficiency** | {self.format_percentage(efficiency_metrics['efficiency_score'].mean() if efficiency_metrics is not None else 0)} | - |

---

## 💰 Pool Performance Breakdown

### TVL by Pool
"""
        
        # Add pool TVL table
        if tvl_snapshot is not None and len(tvl_snapshot) > 0:
            report += """
| Pool | Current TVL | Token 0 | Token 1 | Transactions | Users |
|------|-------------|---------|---------|--------------|-------|
"""
            for _, row in tvl_snapshot.iterrows():
                if row['current_tvl_total'] > 0:
                    report += f"| **{row['pool']}** | {self.format_number(row['current_tvl_total'])} | {row['token0']} | {row['token1']} | {row['total_transactions']:,} | {row['unique_users']:,} |\n"
        
        # Add efficiency metrics
        report += """

### 🏆 Pool Efficiency Rankings
"""
        
        if efficiency_metrics is not None and len(efficiency_metrics) > 0:
            report += """
| Rank | Pool | Efficiency Score | Volume/TVL Ratio | Fee APR |
|------|------|------------------|------------------|---------|
"""
            efficiency_sorted = efficiency_metrics.sort_values('efficiency_score', ascending=False)
            for idx, (_, row) in enumerate(efficiency_sorted.head(5).iterrows(), 1):
                report += f"| {idx} | **{row['pool']}** | {row['efficiency_score']:.1f}/100 | {row['volume_tvl_ratio']:.3f} | {self.format_percentage(row['fee_apr'])} |\n"
        
        # Add trends section
        report += """

---

## 📈 Historical Trends

### 7-Day Highlights
"""
        
        if daily_protocol_tvl is not None and len(daily_protocol_tvl) > 0:
            recent_data = daily_protocol_tvl.tail(7)
            
            report += f"""
- **Peak TVL:** {self.format_number(recent_data['protocol_tvl_total'].max())}
- **Average Daily Volume:** {self.format_number(recent_data['protocol_daily_deposits'].mean() + recent_data['protocol_daily_withdrawals'].mean())}
- **Net Flow (7D):** {self.format_number(recent_data['protocol_daily_net_flow'].sum())}
- **Active Users (7D):** {int(recent_data['protocol_unique_users'].sum()):,}

### Daily TVL Trend (Last 7 Days)
"""
            report += """
| Date | TVL | Daily Change | Deposits | Withdrawals | Net Flow |
|------|-----|--------------|----------|-------------|----------|
"""
            for _, row in recent_data.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
                report += f"| {date_str} | {self.format_number(row['protocol_tvl_total'])} | {self.format_percentage(row.get('protocol_tvl_change_pct', 0))} | {self.format_number(row['protocol_daily_deposits'])} | {self.format_number(row['protocol_daily_withdrawals'])} | {self.format_number(row['protocol_daily_net_flow'])} |\n"
        
        # Add pool-specific insights
        report += """

---

## 💡 Key Insights

### Top Performers
"""
        
        if efficiency_metrics is not None and len(efficiency_metrics) > 0:
            best_pool = efficiency_metrics.loc[efficiency_metrics['efficiency_score'].idxmax()]
            highest_apr_pool = efficiency_metrics.loc[efficiency_metrics['fee_apr'].idxmax()]
            
            report += f"""
1. **Best Overall Pool:** {best_pool['pool']} (Score: {best_pool['efficiency_score']:.1f}/100)
2. **Highest Fee APR:** {highest_apr_pool['pool']} ({self.format_percentage(highest_apr_pool['fee_apr'])})
3. **Most Capital Efficient:** {efficiency_metrics.loc[efficiency_metrics['capital_efficiency'].idxmax()]['pool']}
"""
        
        # Add risk indicators
        if tvl_snapshot is not None and len(tvl_snapshot) > 0:
            total_tvl = tvl_snapshot['current_tvl_total'].sum()
            if total_tvl > 0:
                concentration = tvl_snapshot.nlargest(1, 'current_tvl_total')['current_tvl_total'].sum() / total_tvl * 100
                
                report += f"""

### ⚠️ Risk Indicators
- **TVL Concentration (Top Pool):** {self.format_percentage(concentration)}
- **Pools with TVL < $100k:** {len(tvl_snapshot[tvl_snapshot['current_tvl_total'] < 100000])}
"""
        
        report += """

---

## 📝 Notes
- Data sourced from Mezo protocol subgraphs (Goldsky)
- TVL calculations based on cumulative deposits minus withdrawals
- Efficiency scores factor in capital efficiency, volume/TVL ratio, and fee generation
- All values in USD

---
*This report is automatically generated. For questions, contact the data team.*
"""
        
        return report
    
    def generate_bridge_report(self, metrics: Dict[str, Any], bridge_df: pd.DataFrame):
        """Generate markdown report for bridge data."""

        summary = metrics['summary_overall'].iloc[0]
        current_date = bridge_df['date'].max()

        # Calculate deposit/withdrawal metrics for each timeframe
        timeframes = {
            '24h': bridge_df[bridge_df['date'] == current_date],
            '7d': bridge_df[bridge_df['date'] >= current_date - timedelta(days=7)],
            '30d': bridge_df[bridge_df['date'] >= current_date - timedelta(days=30)],
            'All-time': bridge_df
        }

        # Calculate previous period metrics for % change
        prev_timeframes = {
            '24h': bridge_df[bridge_df['date'] == current_date - timedelta(days=1)],
            '7d': bridge_df[(bridge_df['date'] >= current_date - timedelta(days=14)) &
                           (bridge_df['date'] < current_date - timedelta(days=7))],
            '30d': bridge_df[(bridge_df['date'] >= current_date - timedelta(days=60)) &
                            (bridge_df['date'] < current_date - timedelta(days=30))]
        }

        report = f"""# 🌉 Bridge Analytics Report
*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}*

---

## 📊 Executive Summary

### Key Metrics
| Metric | Value | 7D Change |
|--------|-------|-----------|
| **Current TVL** | {self.format_number(summary['current_tvl'])} | {self.format_percentage(summary['tvl_growth_7d_pct'])} |
| **Net Flow (7d)** | {self.format_number(summary['net_flow_7d'])} | - |
| **24h Active Users** | {int(summary['unique_users_24h']):,} | - |
| **All-time Users** | {int(summary['total_unique_users_all_time']):,} | - |

---

## 📥 Deposits

| Period | Count | Amount | % Change |
|--------|-------|--------|----------|
"""

        # Add deposits data
        for period_name, period_data in timeframes.items():
            deposits = period_data[period_data['type'] == 'deposit']
            deposit_count = len(deposits)
            deposit_amount_usd = deposits['amount_usd'].sum()

            pct_change_str = ""
            if period_name in prev_timeframes:
                prev_deposits = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'deposit']
                prev_amount = prev_deposits['amount_usd'].sum()
                if prev_amount > 0:
                    pct_change = ((deposit_amount_usd - prev_amount) / prev_amount) * 100
                    pct_change_str = f"{pct_change:+.1f}%"
                else:
                    pct_change_str = "N/A"

            report += f"| **{period_name}** | {deposit_count:,} | {self.format_number(deposit_amount_usd)} | {pct_change_str} |\n"

        report += """
---

## 📤 Withdrawals

| Period | Count | Amount | % Change |
|--------|-------|--------|----------|
"""

        # Add withdrawals data
        for period_name, period_data in timeframes.items():
            withdrawals = period_data[period_data['type'] == 'withdrawal']
            withdrawal_count = len(withdrawals)
            withdrawal_amount_usd = withdrawals['amount_usd'].sum()

            pct_change_str = ""
            if period_name in prev_timeframes:
                prev_withdrawals = prev_timeframes[period_name][prev_timeframes[period_name]['type'] == 'withdrawal']
                prev_amount = prev_withdrawals['amount_usd'].sum()
                if prev_amount > 0:
                    pct_change = ((withdrawal_amount_usd - prev_amount) / prev_amount) * 100
                    pct_change_str = f"{pct_change:+.1f}%"
                else:
                    pct_change_str = "N/A"

            report += f"| **{period_name}** | {withdrawal_count:,} | {self.format_number(withdrawal_amount_usd)} | {pct_change_str} |\n"

        report += """
---

## 💱 Net Flow

| Period | Net Count | Net Amount | % Change |
|--------|-----------|------------|----------|
"""

        # Add net flow data
        for period_name, period_data in timeframes.items():
            deposits = period_data[period_data['type'] == 'deposit']
            withdrawals = period_data[period_data['type'] == 'withdrawal']
            net_count = len(deposits) - len(withdrawals)
            net_amount = deposits['amount_usd'].sum() - withdrawals['amount_usd'].sum()

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

            report += f"| **{period_name}** | {net_count:,} | {self.format_number(net_amount)} | {pct_change_str} |\n"

        # Add top tokens section
        report += """
---

## 🪙 Top Tokens by Volume

| Token | Total Volume | Share | Net Flow |
|-------|--------------|-------|----------|
"""

        top_tokens = metrics['summary_by_token'].head(5)
        for _, row in top_tokens.iterrows():
            report += f"| **{row['token']}** | {self.format_number(row['total_volume'])} | {row['volume_share_pct']:.1f}% | {self.format_number(row['net_flow'])} |\n"

        # Add health indicators
        health = metrics['health_metrics'].iloc[0]
        report += f"""
---

## 🏥 Health Status: {health['risk_level']} (Score: {int(health['risk_score'])}/6)

| Indicator | Value |
|-----------|-------|
| **Volatility (30d)** | {health['tvl_volatility_30d']:.1f}% |
| **Max Drawdown (30d)** | {health['max_drawdown_30d']:.1f}% |
| **Consecutive Outflow Days** | {int(health['consecutive_outflow_days'])} |
| **Whale Concentration** | {health['whale_concentration_pct']:.1f}% |
| **Outflow Ratio (7d)** | {health['outflow_ratio_7d']:.2f} |

---

## 📝 Notes
- Data sourced from Mezo Bridge subgraphs
- TVL calculations based on deposits minus withdrawals
- All values in USD
- Health indicators help assess protocol risk

---
*This report is automatically generated. For questions, contact the data team.*
"""

        return report

    def generate_summary_report(self, all_metrics: Dict[str, Any]):
        """Generate a combined summary report for all protocols."""

        report = f"""# 📊 Mezo Protocol Analytics - Daily Summary
*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}*

---

## 🎯 Protocol Overview

"""
        
        # Add pools section if data exists
        if 'pools' in all_metrics:
            pools_data = all_metrics['pools']
            report += f"""
### 🏊 Liquidity Pools
- **Total TVL:** {self.format_number(pools_data.get('total_tvl', 0))}
- **Active Pools:** {pools_data.get('active_pools', 0)}
- **24h Volume:** {self.format_number(0)}  # Add when volume data available
"""
        
        # Placeholder for future protocol sections
        report += """

### 🌉 Bridge (Coming Soon)
- Data pipeline in development

### 💰 MUSD Lending (Coming Soon)
- Data pipeline in development

### 🏦 Vaults (Coming Soon)
- Data pipeline in development

---

*Full protocol coverage coming soon. Individual protocol reports available separately.*
"""
        
        return report

# ==================================================
# MAIN EXECUTION
# ==================================================

# def main():
#     """Main function to generate and upload reports."""
    
#     print("=" * 60)
#     print("📊 MEZO ANALYTICS - REPORT GENERATION")
#     print("=" * 60)
    
#     try:
#         # Load environment variables
#         load_dotenv(dotenv_path='../.env', override=True)
        
#         # Initialize Linear client
#         linear_api_key = os.getenv('LINEAR_API_KEY')
#         if not linear_api_key:
#             raise ValueError("LINEAR_API_KEY not found in environment variables")
        
#         linear = LinearAPIClient(linear_api_key)
#         print("✅ Linear API client initialized")
        
#         # Initialize report generator
#         generator = ReportGenerator()
        
#         # Dictionary to store all metrics
#         all_metrics = {}
        
#         # ==================================================
#         # PROCESS POOLS DATA
#         # ==================================================
        
#         print("\n" + "=" * 60)
#         print("Processing pools data...")
#         print("=" * 60)
        
#         try:
#             # Run the pools processing script
#             pools_results = process_pools()
            
#             if pools_results:
#                 all_metrics['pools'] = pools_results
#                 print(f"✅ Pools data processed successfully")
#                 print(f"   - Total TVL: ${pools_results.get('total_tvl', 0):,.2f}")
#                 print(f"   - Active Pools: {pools_results.get('active_pools', 0)}")
                
#                 # Generate pools report
#                 pools_report = generator.generate_pools_report(pools_results)
                
#                 # Upload to Linear
#                 doc_title = f"Pools Analytics Report - {datetime.now().strftime('%Y-%m-%d')}"
                
#                 # Check if we should update existing doc or create new
#                 existing_doc_id = LINEAR_DOC_ID
                
#                 if existing_doc_id:
#                     # Update existing document
#                     result = linear.update_document(
#                         document_id=existing_doc_id,
#                         content=pools_report,
#                         title=doc_title
#                     )
#                     print(f"✅ Updated Linear document: {result.get('document', {}).get('url')}")
#                 else:
#                     # Create new document
#                     project_id = LINEAR_PROJECT_ID
#                     doc = linear.create_document(
#                         title=doc_title,
#                         content=pools_report,
#                         project_id=project_id
#                     )
#                     print(f"✅ Created Linear document: {doc.get('url')}")
#                     print(f"   Document ID: {doc.get('id')}")
#                     print(f"   (Add LINEAR_DOC_ID={doc.get('id')} to .env to update this doc next time)")
                
#                 # Save report locally as backup
#                 with open(f"reports/pools_report_{datetime.now().strftime('%Y%m%d')}.md", "w") as f:
#                     f.write(pools_report)
#                 print("✅ Report saved locally to reports/ directory")
                
#             else:
#                 print("⚠️ No pools data returned from processing script")
                
#         except Exception as e:
#             print(f"❌ Error processing pools data: {e}")
#             import traceback
#             traceback.print_exc()
        
#         # ==================================================
#         # GENERATE SUMMARY REPORT
#         # ==================================================
        
#         print("\n" + "=" * 60)
#         print("Generating summary report...")
#         print("=" * 60)
        
#         summary_report = generator.generate_summary_report(all_metrics)
        
#         # Upload summary to Linear
#         summary_title = f"Protocol Summary - {datetime.now().strftime('%Y-%m-%d')}"
#         existing_summary_id = LINEAR_SUMMARY_DOC_ID
        
#         if existing_summary_id:
#             result = linear.update_document(
#                 document_id=existing_summary_id,
#                 content=summary_report,
#                 title=summary_title
#             )
#             print(f"✅ Updated summary document: {result.get('document', {}).get('url')}")
#         else:
#             project_id = LINEAR_PROJECT_ID
#             doc = linear.create_document(
#                 title=summary_title,
#                 content=summary_report,
#                 project_id=project_id
#             )
#             print(f"✅ Created summary document: {doc.get('url')}")
#             print(f"   Document ID: {doc.get('id')}")
        
#         # Save summary locally
#         with open(f"reports/summary_report_{datetime.now().strftime('%Y%m%d')}.md", "w") as f:
#             f.write(summary_report)
        
#         print("\n" + "=" * 60)
#         print("✅ REPORT GENERATION COMPLETE")
#         print("=" * 60)
        
#         return all_metrics
        
#     except Exception as e:
#         print(f"\n❌ Critical error: {e}")
#         import traceback
#         traceback.print_exc()
#         raise


# if __name__ == "__main__":
#     results = main()