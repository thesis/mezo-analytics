#!/usr/bin/env python3
"""
Test script to verify that the generic get_subgraph_data function produces 
the same results as the commented out get_all_bridge_transactions function.
"""

import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from scripts.get_raw_data import get_subgraph_data

# Load environment variables
load_dotenv(override=True)

def compare_dataframes(df1, df2, name1, name2):
    """Compare two dataframes and report differences"""
    print(f"\n📊 Comparing {name1} vs {name2}:")
    
    if df1 is None and df2 is None:
        print("✅ Both returned None - MATCH")
        return True
    
    if df1 is None or df2 is None:
        print(f"❌ One is None, other is not - NO MATCH")
        print(f"   {name1}: {'None' if df1 is None else f'{len(df1)} rows'}")
        print(f"   {name2}: {'None' if df2 is None else f'{len(df2)} rows'}")
        return False
    
    # Check shape
    if df1.shape != df2.shape:
        print(f"❌ Different shapes - NO MATCH")
        print(f"   {name1}: {df1.shape}")
        print(f"   {name2}: {df2.shape}")
        return False
    
    # Check columns
    if not df1.columns.equals(df2.columns):
        print(f"❌ Different columns - NO MATCH")
        print(f"   {name1} columns: {list(df1.columns)}")
        print(f"   {name2} columns: {list(df2.columns)}")
        return False
    
    # Check if dataframes are equal
    try:
        if df1.equals(df2):
            print("✅ DataFrames are identical - PERFECT MATCH")
            return True
        else:
            # Find differences
            diff_mask = df1 != df2
            if diff_mask.any().any():
                print("❌ DataFrames have different values - NO MATCH")
                print("   First few differences:")
                for col in df1.columns:
                    if diff_mask[col].any():
                        different_rows = diff_mask[col]
                        first_diff_idx = different_rows.idxmax()
                        print(f"     Column '{col}', row {first_diff_idx}:")
                        print(f"       {name1}: {df1.loc[first_diff_idx, col]}")
                        print(f"       {name2}: {df2.loc[first_diff_idx, col]}")
                        break
                return False
            else:
                print("✅ DataFrames are identical - PERFECT MATCH")
                return True
    except Exception as e:
        print(f"❌ Error comparing dataframes: {e}")
        return False

def test_bridge_transactions():
    """Test bridge transactions data fetching (using generic function vs manual implementation)"""
    print("\n" + "="*60)
    print("🧪 TESTING BRIDGE TRANSACTIONS DATA")
    print("="*60)
    
    # Manual implementation (recreating the commented out function)
    print("\n--- Manual implementation (commented out function) ---")
    try:
        portal = SubgraphClient(
            url=SubgraphClient.MEZO_BRIDGE_SUBGRAPH, 
            headers=SubgraphClient.SUBGRAPH_HEADERS
        )
        bridge = portal.fetch_subgraph_data(
            BridgeQueries.GET_BRIDGE_TRANSACTIONS, 
            'assetsLockeds'
        )
        manual_df = pd.DataFrame(bridge) if bridge else None
        if manual_df is not None:
            print(f"✅ Manual: Found {len(manual_df)} bridge transaction records")
        else:
            print("⚠️ Manual: No bridge transaction data found")
    except Exception as e:
        print(f"❌ Manual implementation failed: {e}")
        manual_df = None
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_bridge = get_subgraph_data(
        SubgraphClient.MEZO_BRIDGE_SUBGRAPH,
        BridgeQueries.GET_BRIDGE_TRANSACTIONS,
        'assetsLockeds'
    )
    
    # Compare results
    return compare_dataframes(manual_df, generic_bridge, "Manual", "Generic")

def main():
    """Run bridge transactions test"""
    print("🚀 TESTING BRIDGE TRANSACTIONS FUNCTION")
    print("=" * 80)
    print("Testing the commented out get_all_bridge_transactions() function")
    print("against the generic get_subgraph_data() function")
    print("=" * 80)
    
    # Test bridge transactions
    success = test_bridge_transactions()
    
    # Summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"Bridge Transactions      {status}")
    
    print("\n" + "="*80)
    if success:
        print("🎉 TEST PASSED! The generic function works identically to the commented out function.")
        print("✅ Safe to use get_subgraph_data() for bridge transactions.")
        print("\n📝 The commented out get_all_bridge_transactions() can be replaced with:")
        print("   get_subgraph_data(")
        print("       SubgraphClient.MEZO_BRIDGE_SUBGRAPH,")
        print("       BridgeQueries.GET_BRIDGE_TRANSACTIONS,")
        print("       'assetsLockeds'")
        print("   )")
    else:
        print("⚠️  TEST FAILED! There are differences between the implementations.")
        print("❌ The generic function does not produce identical results.")
    print("="*80)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)