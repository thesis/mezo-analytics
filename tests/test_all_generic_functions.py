#!/usr/bin/env python3
"""
Test script to verify that the generic get_subgraph_data function produces 
the same results as all remaining individual functions.
"""

import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries, MUSDQueries
from scripts.get_raw_data import (
    get_all_loans, 
    get_liquidation_data, 
    get_trove_liquidated_data,
    get_all_market_donations,
    get_all_market_purchases,
    get_subgraph_data
)

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

def test_loans():
    """Test loans data fetching"""
    print("\n" + "="*60)
    print("🧪 TESTING LOANS DATA")
    print("="*60)
    
    # Original function
    print("\n--- Original get_all_loans() ---")
    original_loans = get_all_loans()
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_loans = get_subgraph_data(
        SubgraphClient.BORROWER_OPS_SUBGRAPH,
        MUSDQueries.GET_LOANS,
        'troveUpdateds'
    )
    
    # Compare results
    return compare_dataframes(original_loans, generic_loans, "Original", "Generic")

def test_liquidations():
    """Test liquidation data fetching"""
    print("\n" + "="*60)
    print("🧪 TESTING LIQUIDATION DATA")
    print("="*60)
    
    # Original function
    print("\n--- Original get_liquidation_data() ---")
    original_liquidations = get_liquidation_data()
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_liquidations = get_subgraph_data(
        SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
        MUSDQueries.GET_MUSD_LIQUIDATIONS,
        'liquidations'
    )
    
    # Compare results
    return compare_dataframes(original_liquidations, generic_liquidations, "Original", "Generic")

def test_trove_liquidations():
    """Test trove liquidation data fetching"""
    print("\n" + "="*60)
    print("🧪 TESTING TROVE LIQUIDATION DATA")
    print("="*60)
    
    # Original function
    print("\n--- Original get_trove_liquidated_data() ---")
    original_trove_liquidations = get_trove_liquidated_data()
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_trove_liquidations = get_subgraph_data(
        SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
        MUSDQueries.GET_LIQUIDATED_TROVES,
        'troveLiquidateds'
    )
    
    # Compare results
    return compare_dataframes(original_trove_liquidations, generic_trove_liquidations, "Original", "Generic")

def test_market_donations():
    """Test market donations data fetching"""
    print("\n" + "="*60)
    print("🧪 TESTING MARKET DONATIONS DATA")
    print("="*60)
    
    # Original function
    print("\n--- Original get_all_market_donations() ---")
    original_donations = get_all_market_donations()
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_donations = get_subgraph_data(
        SubgraphClient.MUSD_MARKET_SUBGRAPH,
        MUSDQueries.GET_MARKET_DONATIONS,
        'donateds'
    )
    
    # Compare results
    return compare_dataframes(original_donations, generic_donations, "Original", "Generic")

def test_market_purchases():
    """Test market purchases data fetching"""
    print("\n" + "="*60)
    print("🧪 TESTING MARKET PURCHASES DATA")
    print("="*60)
    
    # Original function
    print("\n--- Original get_all_market_purchases() ---")
    original_purchases = get_all_market_purchases()
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_purchases = get_subgraph_data(
        SubgraphClient.MUSD_MARKET_SUBGRAPH,
        MUSDQueries.GET_MARKET_PURCHASES,
        'orderPlaceds'
    )
    
    # Compare results
    return compare_dataframes(original_purchases, generic_purchases, "Original", "Generic")

def test_autobridge_transactions():
    """Test autobridge transactions data fetching (using generic function vs manual implementation)"""
    print("\n" + "="*60)
    print("🧪 TESTING AUTOBRIDGE TRANSACTIONS DATA")
    print("="*60)
    
    # Manual implementation (like the commented out function)
    print("\n--- Manual implementation ---")
    try:
        portal = SubgraphClient(
            url=SubgraphClient.MEZO_PORTAL_SUBGRAPH, 
            headers=SubgraphClient.SUBGRAPH_HEADERS
        )
        autobridges = portal.fetch_subgraph_data(
            BridgeQueries.GET_AUTOBRIDGE_TRANSACTIONS, 
            'depositAutoBridgeds'
        )
        manual_df = pd.DataFrame(autobridges) if autobridges else None
        if manual_df is not None:
            print(f"✅ Manual: Found {len(manual_df)} autobridge records")
        else:
            print("⚠️ Manual: No autobridge data found")
    except Exception as e:
        print(f"❌ Manual implementation failed: {e}")
        manual_df = None
    
    # Generic function
    print("\n--- Generic get_subgraph_data() ---")
    generic_autobridges = get_subgraph_data(
        SubgraphClient.MEZO_PORTAL_SUBGRAPH,
        BridgeQueries.GET_AUTOBRIDGE_TRANSACTIONS,
        'depositAutoBridgeds'
    )
    
    # Compare results
    return compare_dataframes(manual_df, generic_autobridges, "Manual", "Generic")

def main():
    """Run all tests"""
    print("🚀 STARTING GENERIC FUNCTION TESTS FOR ALL DATA SOURCES")
    print("=" * 80)
    
    results = []
    
    # Test each function
    results.append(("Loans", test_loans()))
    results.append(("Liquidations", test_liquidations()))
    results.append(("Trove Liquidations", test_trove_liquidations()))
    results.append(("Market Donations", test_market_donations()))
    results.append(("Market Purchases", test_market_purchases()))
    results.append(("Autobridge Transactions", test_autobridge_transactions()))
    
    # Summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:25} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*80)
    if all_passed:
        print("🎉 ALL TESTS PASSED! The generic function works identically to all original functions.")
        print("✅ Safe to refactor all remaining code to use get_subgraph_data().")
        print("\n📝 Ready to replace:")
        print("   • get_all_loans() → get_subgraph_data()")
        print("   • get_liquidation_data() → get_subgraph_data()")
        print("   • get_trove_liquidated_data() → get_subgraph_data()")
        print("   • get_all_market_donations() → get_subgraph_data()")
        print("   • get_all_market_purchases() → get_subgraph_data()")
        print("   • Implement get_all_autobridge_transactions() using get_subgraph_data()")
    else:
        print("⚠️  SOME TESTS FAILED! Do not refactor until issues are resolved.")
        print("❌ The generic function does not produce identical results for all cases.")
    print("="*80)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)