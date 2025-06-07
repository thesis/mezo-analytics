#!/usr/bin/env python3
"""
Script to run the bridge data processing and fix column names for Supabase
"""

from dotenv import load_dotenv
import pandas as pd
import numpy as np
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mezo.clients import SupabaseClient
from mezo.visual_utils import ProgressIndicators
from scripts.process_bridge_data import main as process_bridge_data


def fix_column_names(df):
    """Convert DataFrame column names to lowercase for Supabase compatibility."""
    # Create mapping for column name conversion
    column_mapping = {}
    
    for col in df.columns:
        # Convert specific patterns to lowercase
        if col.startswith('amount_bridged_'):
            token = col.replace('amount_bridged_', '')
            new_col = f'amount_bridged_{token.lower()}'
            column_mapping[col] = new_col
        elif col.startswith('users_'):
            token = col.replace('users_', '')
            new_col = f'users_{token.lower()}'
            column_mapping[col] = new_col
        elif col.startswith('transactions_'):
            token = col.replace('transactions_', '')
            new_col = f'transactions_{token.lower()}'
            column_mapping[col] = new_col
        else:
            # Keep other columns as-is
            column_mapping[col] = col
    
    return df.rename(columns=column_mapping)


def debug_dataframe_types(df, name="DataFrame"):
    """Debug DataFrame types and identify potential issues."""
    ProgressIndicators.print_step(f"Debugging {name} data types", "info")
    
    print(f"  • Shape: {df.shape}")
    print(f"  • Columns: {list(df.columns)}")
    
    # Check for problematic data types
    problematic_columns = []
    
    for col in df.columns:
        dtype = df[col].dtype
        print(f"  • {col}: {dtype}")
        
        # Check for object columns that might contain mixed types
        if dtype == 'object':
            unique_types = set(type(val).__name__ for val in df[col].dropna().head(10))
            if len(unique_types) > 1:
                problematic_columns.append((col, unique_types))
                print(f"    ⚠️  Mixed types found: {unique_types}")
        
        # Check for NaN/None values
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            print(f"    • NaN values: {nan_count}")
        
        # Check for infinity values in numeric columns
        if np.issubdtype(dtype, np.number):
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                problematic_columns.append((col, 'infinity'))
                print(f"    ⚠️  Infinity values: {inf_count}")
    
    if problematic_columns:
        ProgressIndicators.print_step(f"Found {len(problematic_columns)} problematic columns", "warning")
        for col, issue in problematic_columns:
            print(f"    • {col}: {issue}")
    else:
        ProgressIndicators.print_step("No type issues detected", "success")
    
    return problematic_columns


def clean_dataframe_for_supabase(df):
    """Clean DataFrame to ensure Supabase compatibility."""
    ProgressIndicators.print_step("Cleaning DataFrame for Supabase", "start")
    
    cleaned_df = df.copy()
    
    # Replace NaN with None for JSON serialization
    cleaned_df = cleaned_df.replace({np.nan: None})
    
    # Replace infinity values with None
    cleaned_df = cleaned_df.replace([np.inf, -np.inf], None)
    
    # Convert object columns to appropriate types
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object' and col != 'timestamp_':
            try:
                # Try to convert to numeric if it's not the timestamp column
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
            except:
                pass
    
    # Ensure timestamp column is string
    if 'timestamp_' in cleaned_df.columns:
        cleaned_df['timestamp_'] = cleaned_df['timestamp_'].astype(str)
    
    ProgressIndicators.print_step("DataFrame cleaning completed", "success")
    return cleaned_df


def main():
    """Process bridge data and fix column names for Supabase."""
    ProgressIndicators.print_header("BRIDGE DATA PROCESSING WITH COLUMN FIX")
    
    try:
        # Load environment
        load_dotenv(dotenv_path='../.env', override=True)
        ProgressIndicators.print_step("Environment loaded", "success")
        
        # Run the bridge data processing
        ProgressIndicators.print_step("Running bridge data processing pipeline", "start")
        results = process_bridge_data()
        ProgressIndicators.print_step("Bridge data processing completed", "success")
        
        # Get the daily bridge data
        daily_bridge_data = results['daily_bridge_data']
        ProgressIndicators.print_step(f"Retrieved daily data: {daily_bridge_data.shape}", "success")
        
        # Debug the original data
        debug_dataframe_types(daily_bridge_data, "Original Daily Bridge Data")
        
        # Fix column names
        ProgressIndicators.print_step("Converting column names to lowercase", "start")
        fixed_data = fix_column_names(daily_bridge_data)
        
        print(f"  • Original columns: {len(daily_bridge_data.columns)}")
        print(f"  • Fixed columns: {len(fixed_data.columns)}")
        print(f"  • Sample mapping: amount_bridged_DAI → amount_bridged_dai")
        
        ProgressIndicators.print_step("Column names converted", "success")
        
        # Clean the data for Supabase compatibility
        cleaned_data = clean_dataframe_for_supabase(fixed_data)
        
        # Debug the cleaned data
        debug_dataframe_types(cleaned_data, "Cleaned Data for Supabase")
        
        # Upload to Supabase
        ProgressIndicators.print_step("Uploading to Supabase", "start")
        supabase = SupabaseClient()
        
        # Clear existing data first
        try:
            supabase.supabase_insert.table('mainnet_daily_bridge_data').delete().neq('id', 0).execute()
            ProgressIndicators.print_step("Cleared existing data", "info")
        except Exception as clear_error:
            ProgressIndicators.print_step(f"Clear data note: {clear_error}", "info")
        
        # Upload new data with detailed error handling
        try:
            upload_result = supabase.update_supabase('mainnet_daily_bridge_data', cleaned_data)
        except Exception as upload_error:
            ProgressIndicators.print_step(f"Upload failed: {upload_error}", "error")
            
            # Try to identify the specific issue
            print(f"\n{ProgressIndicators.INFO} Upload error analysis:")
            print(f"  • Error type: {type(upload_error).__name__}")
            print(f"  • Error message: {str(upload_error)}")
            
            # Try uploading just the first row to isolate the issue
            ProgressIndicators.print_step("Testing upload with single row", "start")
            try:
                test_row = cleaned_data.head(1)
                test_result = supabase.update_supabase('mainnet_daily_bridge_data', test_row)
                ProgressIndicators.print_step("Single row upload successful", "success")
                ProgressIndicators.print_step("Issue may be with data volume or specific rows", "warning")
                
                # Try smaller batches
                batch_size = 5
                ProgressIndicators.print_step(f"Trying batch upload with size {batch_size}", "start")
                
                all_results = []
                for i in range(0, len(cleaned_data), batch_size):
                    batch = cleaned_data.iloc[i:i+batch_size]
                    try:
                        batch_result = supabase.update_supabase('mainnet_daily_bridge_data', batch)
                        all_results.extend(batch_result)
                        ProgressIndicators.print_step(f"Batch {i//batch_size + 1} uploaded", "success")
                    except Exception as batch_error:
                        ProgressIndicators.print_step(f"Batch {i//batch_size + 1} failed: {batch_error}", "error")
                        print(f"  • Problematic batch rows: {i} to {i+batch_size}")
                        debug_dataframe_types(batch, f"Failed Batch {i//batch_size + 1}")
                        raise
                
                upload_result = all_results
                
            except Exception as test_error:
                ProgressIndicators.print_step(f"Single row test also failed: {test_error}", "error")
                raise
        ProgressIndicators.print_step(f"Successfully uploaded {len(upload_result)} rows", "success")
        
        # Verify the upload
        verify_result = supabase.supabase_insert.table('mainnet_daily_bridge_data').select('*').execute()
        
        ProgressIndicators.print_summary_box(
            f"{ProgressIndicators.DATABASE} UPLOAD SUMMARY",
            {
                "Table": "mainnet_daily_bridge_data",
                "Rows Uploaded": len(upload_result),
                "Total Rows in Table": len(verify_result.data),
                "Date Range": f"{fixed_data['timestamp_'].min()} to {fixed_data['timestamp_'].max()}",
                "Columns": len(fixed_data.columns)
            }
        )
        
        ProgressIndicators.print_header(f"{ProgressIndicators.SUCCESS} UPLOAD COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        ProgressIndicators.print_step(f"Error: {e}", "error")
        import traceback
        print(f"\n{ProgressIndicators.INFO} Error details:")
        traceback.print_exc()


if __name__ == "__main__":
    main()