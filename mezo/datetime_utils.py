from datetime import datetime, timezone
import pandas as pd

def convert_unix_to_datetime(df, columns):
    def is_valid_unix_timestamp(value):
        return isinstance(value, (int, float)) and (1e9 <= value <= 1e13)

    def convert_timestamp(value):
        if is_valid_unix_timestamp(value):
            if value > 1e12:
                value = value / 1000 
            return datetime.fromtimestamp(value, tz=timezone.utc)
        else:
            return pd.to_datetime(value, errors='coerce')

    for column in columns:
        df[column] = df[column].apply(convert_timestamp)
    
    return df

def format_datetimes(df, date_columns):
    df[date_columns] = df[date_columns].astype(float)
    df = convert_unix_to_datetime(df, date_columns)
    df[date_columns] = df[date_columns].apply(lambda col: pd.to_datetime(col).dt.date)
    
    return df

def groupby_date(df: pd.DataFrame, date_column='date', agg_dict=None) -> pd.DataFrame:
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in DataFrame.")
    
    if not agg_dict:
        raise ValueError("Aggregation dictionary (agg_dict) cannot be empty.")

    daily_df = df.groupby([date_column]).agg(agg_dict).reset_index()
    
    return daily_df