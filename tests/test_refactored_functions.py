#!/usr/bin/env python3
"""
Test script to verify that the refactored generic function produces 
the same results as the original individual functions.
"""

import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mezo.clients import SubgraphClient
from mezo.queries import MUSDQueries
from scripts.get_raw_data import get_all_loans, get_liquidation_data, get_trove_liquidated_data

# Load environment variables
load_dotenv(override=True)

def fetch_musd_data_generic(subgraph_url, query, query_key, data_type):
    """Generic function to fetch MUSD data from subgraphs"""
    musd = SubgraphClient(url=subgraph_url, headers=SubgraphClient.SUBGRAPH_HEADERS)
    
    print(f"🔍 Trying {data_type} query...")
    try:
        data = musd.fetch_subgraph_data(query, query_key)
        if data:
            df = pd.DataFrame(data)
            print(f"✅ Found {len(df)} {data_type} records")
            return df
        else:
            print(f"⚠️ {data_type} query returned no data")
            return None
    except Exception as e:
        print(f"❌ {data_type} query failed: {e}")
        return None

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
    print("\n--- Generic function (loans) ---")
    generic_loans = fetch_musd_data_generic(
        subgraph_url=SubgraphClient.BORROWER_OPS_SUBGRAPH,
        query=MUSDQueries.GET_LOANS,
        query_key='troveUpdateds',
        data_type='loan'
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
    print("\n--- Generic function (liquidations) ---")
    generic_liquidations = fetch_musd_data_generic(
        subgraph_url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
        query=MUSDQueries.GET_MUSD_LIQUIDATIONS,
        query_key='liquidations',
        data_type='liquidation'
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
    print("\n--- Generic function (trove liquidations) ---")
    generic_trove_liquidations = fetch_musd_data_generic(
        subgraph_url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
        query=MUSDQueries.GET_LIQUIDATED_TROVES,
        query_key='troveLiquidateds',
        data_type='trove liquidation'
    )
    
    # Compare results
    return compare_dataframes(original_trove_liquidations, generic_trove_liquidations, "Original", "Generic")

def main():
    """Run all tests"""
    print("🚀 STARTING REFACTORED FUNCTION TESTS")
    print("=" * 80)
    
    results = []
    
    # Test each function
    results.append(("Loans", test_loans()))
    results.append(("Liquidations", test_liquidations()))
    results.append(("Trove Liquidations", test_trove_liquidations()))
    
    # Summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:20} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*80)
    if all_passed:
        print("🎉 ALL TESTS PASSED! The generic function works identically to the original functions.")
        print("✅ Safe to refactor the original code.")
    else:
        print("⚠️  SOME TESTS FAILED! Do not refactor until issues are resolved.")
        print("❌ The generic function does not produce identical results.")
    print("="*80)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)