import pandas as pd
from datetime import datetime

def save_raw_data(df, suffix):
    path = '/Users/laurenjackson/Desktop/mezo-analytics-mainnet/data/raw/'
    updated_on = datetime.today().date()
    df.to_csv(f'{path}/{updated_on}_{suffix}')

def load_raw_data(filename):
    path = f"/Users/laurenjackson/Desktop/mezo-analytics-mainnet/data/raw/"
    df = pd.read_csv(f'{path}/{filename}')
    
    return df

def add_cumulative_columns(df, cols):
    # First, add the cumulative columns
    df_with_cumulative = df.assign(**{
        f'cumulative_{col}': df[col].cumsum()
        for col in cols
    })
    
    # Then, add the growth columns using the updated dataframe
    df_final = df_with_cumulative.assign(**{
        f'cumulative_{col}_growth': df_with_cumulative[f'cumulative_{col}'].pct_change()
        for col in cols
    })
    
    return df_final
    
def add_pct_change_columns(df, cols, interval):
    return df.assign(**{
        f'{interval}_{col}_change': df[col].pct_change()
        for col in cols
    })

def add_rolling_values(df, window, cols):
    return df.assign(**{
        f'rolling_{col}_{window}': df[col].rolling(window, min_periods=1).mean()
        for col in cols
    })
