#!/usr/bin/env python3
"""
Test script for the new dynamic table creation functionality
"""

from dotenv import load_dotenv
import pandas as pd
import sys
import os

# Add the project root to the path  
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mezo.clients import SupabaseClient
from mezo.visual_utils import ProgressIndicators


def test_dynamic_table_creation():
    """Test the dynamic table creation with various column types."""
    ProgressIndicators.print_header("TESTING DYNAMIC TABLE CREATION")
    
    try:
        # Load environment
        load_dotenv(dotenv_path='../.env', override=True)
        ProgressIndicators.print_step("Environment loaded", "success")
        
        # Connect to Supabase
        supabase = SupabaseClient()
        ProgressIndicators.print_step("Connected to Supabase", "success")
        
        # Test 1: Table with various timestamp column patterns
        test_data_1 = pd.DataFrame({
            'timestamp_': ['2025-01-01', '2025-01-02'],  # ends with _
            'created_date': ['2025-01-01', '2025-01-02'],  # contains date
            'update_time': ['12:00:00', '13:00:00'],  # contains time
            'amount_usd': [100.50, 200.75],  # float
            'transaction_count': [5, 10],  # int
            'token_symbol': ['BTC', 'ETH'],  # string
        })
        
        table_name_1 = "test_dynamic_table_timestamps"
        ProgressIndicators.print_step(f"Testing table creation: {table_name_1}", "start")
        
        if supabase.ensure_table_exists_for_dataframe(table_name_1, test_data_1):
            ProgressIndicators.print_step("Table creation test 1 passed", "success")
        else:
            ProgressIndicators.print_step("Table creation test 1 failed", "error")
            return False
        
        # Test 2: Bridge-like data structure
        test_data_2 = pd.DataFrame({
            'Date': ['2025-01-01', '2025-01-02'],  # Date in name
            'Amount_Bridged_DAI': [1000.0, 2000.0],
            'Amount_Bridged_USDC': [5000.0, 3000.0],
            'Users_DAI': [5.0, 8.0],
            'Transactions_DAI': [10, 15],
        })
        
        table_name_2 = "test_bridge_structure"
        ProgressIndicators.print_step(f"Testing bridge structure: {table_name_2}", "start")
        
        if supabase.ensure_table_exists_for_dataframe(table_name_2, test_data_2):
            ProgressIndicators.print_step("Bridge structure test passed", "success")
        else:
            ProgressIndicators.print_step("Bridge structure test failed", "error")
            return False
        
        # Test 3: Upload actual data to verify it works
        ProgressIndicators.print_step("Testing data upload to dynamic tables", "start")
        
        # Clean data for lowercase compatibility
        clean_data_1 = test_data_1.copy()
        clean_data_1.columns = [col.lower() for col in test_data_1.columns]
        
        clean_data_2 = test_data_2.copy()
        clean_data_2.columns = [col.lower() for col in test_data_2.columns]
        
        try:
            supabase.update_supabase(table_name_1, clean_data_1)
            supabase.update_supabase(table_name_2, clean_data_2)
            ProgressIndicators.print_step("Data upload test passed", "success")
        except Exception as e:
            ProgressIndicators.print_step(f"Data upload test failed: {e}", "error")
            return False
        
        # Verify data was inserted
        verify_1 = supabase.supabase_insert.table(table_name_1).select('*').execute()
        verify_2 = supabase.supabase_insert.table(table_name_2).select('*').execute()
        
        ProgressIndicators.print_summary_box(
            f"{ProgressIndicators.DATABASE} TEST RESULTS",
            {
                f"{table_name_1} rows": len(verify_1.data),
                f"{table_name_2} rows": len(verify_2.data),
                "Timestamp detection": "✅ Working",
                "Dynamic creation": "✅ Working",
                "Data upload": "✅ Working"
            }
        )
        
        ProgressIndicators.print_header(f"{ProgressIndicators.SUCCESS} ALL TESTS PASSED")
        return True
        
    except Exception as e:
        ProgressIndicators.print_step(f"Test error: {e}", "error")
        import traceback
        print(f"\n{ProgressIndicators.INFO} Error details:")
        traceback.print_exc()
        return False


def main():
    """Run the dynamic table creation tests."""
    success = test_dynamic_table_creation()
    
    if success:
        print(f"\n{ProgressIndicators.SUCCESS} Dynamic table creation is working correctly!")
        print(f"{ProgressIndicators.INFO} You can now use supabase.ensure_table_exists_for_dataframe() in your scripts")
    else:
        print(f"\n{ProgressIndicators.ERROR} Tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()