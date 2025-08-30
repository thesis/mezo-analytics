#!/usr/bin/env python3
"""
MUSD Market Txn Fee Data Fetcher

This script fetches transaction fees data from the 
MUSD Market smart contracts via the Mezo block explorer API.
"""

import pandas as pd
import requests
import time
from typing import Dict, List, Tuple

CONTRACTS = {
    "Store": "0xB6881e8b21a3cd6D23c4F90724E26e35BB8980bE",
    "Donations": "0x6aD9E8e5236C0E2cF6D755Bb7BE4eABCbC03f76d"
}

MARKET_MAP = {
    'Brink': 'Brink',
    'SheFi': 'SheFi',
    '1001': 'ledger_nano_x',
    '1002': 'ledger_stax', 
    '1003': 'bitrefill_25',
    '1004': 'bitrefill_50',
    '1005': 'bitrefill_100', 
    '1006': 'bitrefill_200',
    '1007': 'bitrefill_1000'
}


def fetch_contract_transactions(contract_name: str, contract_address: str) -> List[Dict]:
    """
    Fetch transactions and fee data from the MUSD Market smart contracts
    
    Args:
        contract_name: Human-readable name for the contract
        contract_address: contract address
        
    Returns:
        List of transaction dictionaries
    """
    print(f"\n{'='*60}")
    print(f"📡 Fetching transactions for {contract_name}")
    print(f"📍 Contract: {contract_address}")
    print(f"{'='*60}")
    
    url = f"https://api.explorer.mezo.org/api/v2/addresses/{contract_address}/transactions"
    all_transactions = []
    seen_hashes = set()
    next_page_params = None
    page_count = 0

    while True:
        page_count += 1
        print(f"Fetching page {page_count} for {contract_name}...")
        
        # Use cursor-based pagination
        if next_page_params:
            response = requests.get(url, params=next_page_params)
        else:
            response = requests.get(url)
        
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code}")
            break
        
        data = response.json()
        transactions = data.get("items", [])
        
        if not transactions:
            print(f"No more transactions found for {contract_name}")
            break
        
        print(f"Found {len(transactions)} transactions on page {page_count}")
        
        # Process each transaction
        new_count = 0
        for tx in transactions:
            tx_hash = tx.get('hash')
            
            # Skip duplicates (safety check)
            if tx_hash in seen_hashes:
                continue
            
            seen_hashes.add(tx_hash)
            new_count += 1
            
            tx_data = {
                'contract_name': contract_name,
                'contract_address': contract_address,
                'timestamp_': tx.get('timestamp'),
                'method': tx.get('method'),
                'fee_value': int(tx.get('fee', {}).get('value', 0)) if tx.get('fee') else 0,
                'has_error': tx.get('has_error_in_internal_txs', False),
                'from_address': tx.get('from', {}).get('hash') if tx.get('from') else None,
                'to_address': tx.get('to', {}).get('hash') if tx.get('to') else None,
                'transactionHash_': tx_hash,
                'block_number': tx.get('block'),
            }

            # Extract decoded input parameters if available
            if tx.get('decoded_input') and tx.get('decoded_input').get('parameters'):
                parameters = tx.get('decoded_input').get('parameters')
                if len(parameters) > 0:
                    first_param = parameters[0]
                    tx_data['param_0_name'] = first_param.get('name')
                    tx_data['param_0_value'] = str(first_param.get('value'))
            
            all_transactions.append(tx_data)
        
        print(f"✅ Added {new_count} new transactions for {contract_name}")
        
        # Get next page cursor
        next_page_params = data.get("next_page_params")
        
        if not next_page_params:
            print(f"📄 Reached last page for {contract_name}")
            break
        
        # Be respectful to the API
        time.sleep(0.1)
    
    # Contract summary
    print(f"\n📊 {contract_name} Summary:")
    print(f"  Total pages: {page_count}")
    print(f"  Total transactions: {len(all_transactions)}")
    
    return all_transactions

def process_market_data(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the transaction data for analysis.
    
    Args:
        transactions_df: Raw transactions DataFrame
        
    Returns:
        Processed DataFrame with market-specific columns
    """
    # Convert fee values from wei to ETH and format to avoid scientific notation
    transactions_df['fee_value'] = (transactions_df['fee_value'] / 1e18).round(10)
    
    # Convert timestamps to datetime and extract date
    transactions_df['timestamp_'] = pd.to_datetime(transactions_df['timestamp_'])
    transactions_df['date'] = transactions_df['timestamp_'].dt.date
    
    market_methods = ['orderWithPermit', 'donateWithPermit']
    market_df = transactions_df[transactions_df['method'].isin(market_methods)].copy()
    market_df['market_item'] = market_df['param_0_value'].map(MARKET_MAP)
    
    # Add transaction type for clarity
    market_df['transaction_type'] = market_df['method'].map({
        'orderWithPermit': 'purchase',
        'donateWithPermit': 'donation'
    })
    
    return market_df


def generate_summary_statistics(transactions_df: pd.DataFrame) -> Dict:
    """
    Generate summary statistics for the transaction data.
    
    Args:
        transactions_df: Processed transactions DataFrame
        
    Returns:
        Dictionary containing summary statistics
    """
    stats = {
        'total_transactions': len(transactions_df),
        'contracts_analyzed': transactions_df['contract_name'].nunique(),
        'date_range': f"{transactions_df['timestamp_'].min()} to {transactions_df['timestamp_'].max()}",
        'total_fees_eth': transactions_df['fee_value'].sum(),
        'unique_methods': transactions_df['method'].nunique(),
        'transactions_by_contract': transactions_df['contract_name'].value_counts().to_dict(),
        'methods_breakdown': transactions_df['method'].value_counts().to_dict()
    }
    
    return stats


def main() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Main function to fetch and process market transaction data.
    
    Returns:
        Tuple of (combined_df, store_df, donations_df)
    """
    print("🚀 Starting Market Transactions Data Collection")
    print("=" * 60)
    
    # Set pandas display options to avoid scientific notation
    pd.set_option('display.float_format', '{:.10f}'.format)
    
    # Store all transactions from both contracts
    all_transactions = []
    
    # Loop through both contracts
    for contract_name, contract_address in CONTRACTS.items():
        contract_transactions = fetch_contract_transactions(contract_name, contract_address)
        all_transactions.extend(contract_transactions)
        
        # Add delay between contracts
        if contract_name != list(CONTRACTS.keys())[-1]:
            print(f"\n⏳ Waiting before fetching next contract...")
            time.sleep(1)
    
    # Create combined DataFrame
    print(f"\n{'='*60}")
    print(f"📊 COMBINED ANALYSIS")
    print(f"{'='*60}")
    
    if all_transactions:
        # Create and process main DataFrame
        transactions_df = pd.DataFrame(all_transactions)
        transactions_df = process_market_data(transactions_df)
        
        # Sort by timestamp (most recent first)
        transactions_df = transactions_df.sort_values('timestamp_', ascending=False)
        
        # Generate summary statistics
        stats = generate_summary_statistics(transactions_df)
        
        # Print summary
        print(f"✅ Total transactions collected: {stats['total_transactions']}")
        print(f"📍 Contracts analyzed: {stats['contracts_analyzed']}")
        print(f"📅 Overall date range: {stats['date_range']}")
        print(f"🔧 Unique methods across all contracts: {stats['unique_methods']}")
        print(f"💰 Total fees paid: {stats['total_fees_eth']:.8f} ETH")
        
        print(f"\n📊 Transactions by contract:")
        for contract, count in stats['transactions_by_contract'].items():
            print(f"  {contract}: {count}")
        
        print(f"\n📈 Top methods across all contracts:")
        for method, count in list(stats['methods_breakdown'].items())[:10]:
            print(f"  {method}: {count}")
        
        print(f"\n🔍 Sample transactions from combined dataset:")
        sample_cols = ['contract_name', 'date', 'method', 'market_item', 'fee_value']
        print(transactions_df[sample_cols].head(10).to_string(index=False))
        
        # Create individual contract DataFrames
        store_df = transactions_df[transactions_df['contract_name'] == 'Store'].copy()
        donations_df = transactions_df[transactions_df['contract_name'] == 'Donations'].copy()
        
        print(f"\n💡 Individual DataFrames created:")
        print(f"  store_df: {len(store_df)} transactions")
        print(f"  donations_df: {len(donations_df)} transactions")
        print(f"  transactions_df: {len(transactions_df)} transactions (combined)")
        
        print(f"\n🎉 Multi-contract data collection complete!")
        
        return transactions_df, store_df, donations_df
        
    else:
        print("❌ No transactions collected from any contract")
        empty_df = pd.DataFrame()
        return empty_df, empty_df, empty_df


if __name__ == "__main__":
    # Execute the main function
    combined_transactions, store_transactions, donations_transactions = main()