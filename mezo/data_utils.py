from datetime import datetime

import pandas as pd


def add_cumulative_columns(df, cols):
    # First, add the cumulative columns
    df_with_cumulative = df.assign(**{
        f'cumulative_{col}': df[col].cumsum()
        for col in cols
    })
    
    # Then, add the growth columns using the updated dataframe
    df_final = df_with_cumulative.assign(**{
        f'cumulative_{col}_growth': df_with_cumulative[f'cumulative_{col}'].pct_change(periods = 7)
        for col in cols
    })
    
    return df_final
    
def add_pct_change_columns(df, cols, interval):
    return df.assign(**{
        f'{interval}_{col}_change': df[col].pct_change(periods = 7)
        for col in cols
    })

def add_rolling_values(df, window, cols):
    return df.assign(**{
        f'rolling_{col}_{window}': df[col].rolling(window, min_periods=1).mean()
        for col in cols
    })

def add_pool_volume_columns(df, in_suffix='_in', out_suffix='_out'):
    """
    Add volume columns for each pool in a pivoted daily swaps dataframe.
    
    Args:
        df: DataFrame with columns like 'musd_in_POOL1', 'musd_out_POOL1', etc.
        in_suffix: Suffix for inflow columns (default: '_in')
        out_suffix: Suffix for outflow columns (default: '_out')
    
    Returns:
        DataFrame with additional 'volume_POOL' columns
    """
    # Extract pool names from column names in one pass
    in_pools = {col.replace(f'musd{in_suffix}_', '') for col in df.columns if col.startswith(f'musd{in_suffix}_')}
    out_pools = {col.replace(f'musd{out_suffix}_', '') for col in df.columns if col.startswith(f'musd{out_suffix}_')}
    
    # Only create volume columns for pools that have both in and out columns
    pools_with_both = in_pools & out_pools
    
    # Create volume columns using dictionary comprehension
    volume_columns = {
        f'volume_{pool}': df[f'musd{in_suffix}_{pool}'] + df[f'musd{out_suffix}_{pool}']
        for pool in pools_with_both
    }
    
    return df.assign(**volume_columns)

def flatten_json_column(df, json_col, prefix=None):
    """
    Flatten any JSON column into separate columns using pd.json_normalize
    
    Args:
        df: DataFrame with column containing JSON data
        json_col: Name of the column containing JSON data
        prefix: Optional prefix for new column names (defaults to json_col + '_')
    
    Returns:
        DataFrame with flattened JSON data as new columns
    """
    if json_col not in df.columns:
        raise ValueError(f"Column '{json_col}' not found in DataFrame")
    
    # Set default prefix
    if prefix is None:
        prefix = f"{json_col}_"
    
    # Normalize the JSON data
    json_normalized = pd.json_normalize(df[json_col])
    
    # Create the result DataFrame starting with original data
    result_df = df.copy()
    
    # Add all flattened columns with prefix
    for col in json_normalized.columns:
        # Replace dots with underscores for cleaner column names
        # Convert to string first in case column names are integers
        clean_col_name = str(col).replace('.', '_')
        new_col_name = f"{prefix}{clean_col_name}"
        result_df[new_col_name] = json_normalized[col]
    
    result_df = result_df.drop(columns=[json_col])
    
    return result_df