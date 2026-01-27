from datetime import datetime, timezone
import pandas as pd

def convert_unix_to_datetime(df, columns):
    def is_valid_unix_timestamp(value):
        return isinstance(value, (int, float)) and (1e9 <= value <= 1e16)

    def convert_timestamp(value):
        if is_valid_unix_timestamp(value):
            # Handle microseconds (16 digits)
            if value > 1e15:
                value = value / 1e6
            # Handle milliseconds (13 digits) 
            elif value > 1e12:
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
    
    # Convert to date, handling mixed tz-aware/tz-naive values
    for col in date_columns:
        # Convert to datetime, coercing errors, then normalize timezone
        dt_series = pd.to_datetime(df[col], errors='coerce', utc=True)
        # Extract date (returns tz-naive date objects)
        df[col] = dt_series.dt.date
    
    return df

def groupby_date(df: pd.DataFrame, date_column='date', agg_dict=None) -> pd.DataFrame:
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in DataFrame.")
    
    if not agg_dict:
        raise ValueError("Aggregation dictionary (agg_dict) cannot be empty.")

    daily_df = df.groupby([date_column]).agg(agg_dict).reset_index()
    
    return daily_df

def groupby_week(df: pd.DataFrame, date_column, agg_dict=None) -> pd.DataFrame:
    if date_column not in df.columns:
      raise ValueError(f"Column '{date_column}' not found in DataFrame.")    
    if not agg_dict:
        raise ValueError("Aggregation dictionary (agg_dict) cannot be empty.")

    weekly_df = df.groupby(pd.Grouper(key=date_column, freq='W')).agg(agg_dict).reset_index()

    return weekly_df