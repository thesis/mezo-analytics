import pandas as pd
import os
from datetime import datetime

from mezo.clients import SupabaseClient, SubgraphClient
from scripts.get_raw_data import get_all_loans

pd.options.display.float_format = '{:.5f}'.format

updated_on = datetime.today().date()
filename = f'{updated_on}_musd_loans.csv'
csv_path = os.path.join(os.path.dirname("__file__"), '.', 'data', 'raw', filename)
csv_path = os.path.abspath(csv_path)  # converts to absolute path
print(csv_path)
raw_loans = pd.read_csv(csv_path)


def created_merged_loans(df_new, df_all, musd: SubgraphClient):
    """Merges DFs from get_new_loans() and get_all_loans() to get data for new loans only"""
    processor = DataProcessor(
    columns_to_keep=['timestamp__x', 'borrower_x', 'principal', 'coll', 'stake', 
                     'interest', 'transactionHash_'],
    currency_columns=['collateral', 'stake']
    )

    rename_map = {
            'timestamp__x': 'date',
            'borrower_x': 'borrower',
            'principal': 'loan_amt',
            'coll': 'collateral',
            'stake': 'stake',
            'interest': 'interest',
            'transactionHash_': 'txn_hash',
        }

    df = pd.merge(df_all, df_new, on='transactionHash_', how='inner')
    df = processor.keep_and_rename_columns(df, rename_map)
    processor.format_currency_columns(df, ['loan_amt', 'collateral', 'stake', 'interest'])
    
    df['count'] = 1

    return df

def get_edited_loans(df: pd.DataFrame, musd:SubgraphClient, supabase: SupabaseClient, update_db: bool = True):
    """Query pulls data for EDITED loans from BorrowerOps contract 
    by finding records where operation = 2"""
    edited_loans = df.loc[df['operation'] == "2"]
    new_loans = df.loc[df['operation'] == "0"]

    edited_loans_with_original_data = pd.merge(edited_loans, new_loans, on='borrower', how='left')

    processor = DataProcessor(
        columns_to_keep=['timestamp__x', 'borrower', 'principal_x', 'coll_x', 
                         'stake_x', 'interest_x', 'timestamp__y', 'principal_y', 
                         'coll_y', 'stake_y', 'interest_y'])

    rename_map = {
            'timestamp__x': 'updated_on',
            'principal_x': 'new_loan_amt',
            'coll_x': 'new_coll',
            'stake_x': 'new_stake',
            'interest_x': 'new_interest',
            'timestamp__y': 'opened_on',
            'principal_y': 'original_loan_amt',
            'coll_y': 'original_coll',
            'stake_y': 'original_stake',
            'interest_y': 'original_interest',
        }

    edited_loans_final = processor.keep_and_rename_columns(edited_loans_with_original_data, rename_map)

    edited_loans_final = processor.format_currency_columns(
        edited_loans_final, 
        ['new_loan_amt', 'new_coll', 'new_stake','new_interest', 
         'original_loan_amt', 'original_coll', 'original_stake', 'original_interest'])

    edited_loans_final = edited_loans_final[[
            'opened_on', 'updated_on', 'original_loan_amt', 'new_loan_amt',
            'original_coll', 'new_coll', 'original_interest', 'new_interest',
            'original_stake', 'new_stake', 'borrower']]
    
    date_cols = ['opened_on', 'updated_on']
    edited_loans_final = processor.transform_dates(edited_loans_final, date_cols)

    return edited_loans_final

def process_daily_borrows(df: pd.DataFrame, supabase: SupabaseClient, update_db: bool = True):
    daily_df = df.groupby(['date']).agg(
        borrows = ('count', 'sum'),
        borrowers = ('borrower', lambda x: x.drop_duplicates().count()),
        loan_amt =('loan_amt', 'sum'),
        collateral = ('collateral', 'sum'),
        stake = ('stake', 'sum'),
        interest = ('interest', 'sum')
    ).reset_index()

    daily_df['cumulative_loan_amt'] = daily_df['loan_amt'].cumsum()
    daily_df['cumulative_loan_growth'] = daily_df['cumulative_loan_amt'].pct_change()

    daily_df['cumulative_borrows'] = daily_df['borrows'].cumsum()
    daily_df['cumulative_borrow_growth'] = daily_df['cumulative_borrows'].pct_change()

    daily_df['cumulative_borrowers'] = daily_df['borrowers'].cumsum()
    daily_df['cumulative_borrower_growth'] = daily_df['cumulative_borrowers'].pct_change()
    
    # Handle missing and infinite values
    growth_cols = ['cumulative_loan_growth', 'cumulative_borrow_growth', 'cumulative_borrowers']
    daily_df[growth_cols].replace([float('inf'), -float('inf')], 0, inplace=True)
    daily_df[growth_cols].fillna(0, inplace=True)

    # Rolling Growth Averages
    long_window = 30

    daily_df['loan_amt_rolling_30'] = daily_df['loan_amt'].rolling(window=long_window, min_periods=1).mean()
    daily_df['borrowers_rolling_30'] = daily_df['borrowers'].rolling(window=long_window, min_periods=1).mean()

    daily_df['date'] = daily_df['date'].astype(str)
    daily_df['loan_amt'] = daily_df['loan_amt'].astype(float)
    daily_df['cumulative_loan_amt'] = daily_df['cumulative_loan_amt'].astype(float)
    if update_db:
        supabase.update_supabase('daily_borrows_v2', daily_df)

    return daily_df

def process_weekly_borrows(df: pd.DataFrame, supabase: SupabaseClient, update_db: bool = True):
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    df_weekly = df.resample('W', label='left', closed='left').agg({
        'loan_amt': 'sum',
        'borrows': 'sum',
        'borrowers': 'sum',
        'collateral': 'sum',
        'cumulative_loan_amt': 'last',
        'cumulative_borrows': 'last',
        'cumulative_borrowers': 'last'
    }).reset_index()

    df_weekly['wow_loan_amt_change'] = df_weekly['loan_amt'].pct_change()
    df_weekly['wow_borrows_change'] = df_weekly['borrows'].pct_change()
    df_weekly['wow_borrowers_change'] = df_weekly['borrowers'].pct_change()

    df_weekly['date'] = df_weekly['date'].astype(str)
    df_weekly['loan_amt'] = df_weekly['loan_amt'].astype(float)
    df_weekly['cumulative_loan_amt'] = df_weekly['cumulative_loan_amt'].astype(float)
    
    
    if update_db:
        supabase.update_supabase('weekly_borrows', df_weekly)

def process_borrow_summary(df: pd.DataFrame, daily_df: pd.DataFrame, supabase: SupabaseClient):
    loan_average = df['loan_amt'].mean()
    loan_median = df['loan_amt'].median()
    collateral_average = df['collateral'].mean()
    daily_loan_average = daily_df['loan_amt'].mean()
    daily_borrows_average = daily_df['borrows'].mean()
    daily_borrower_average = daily_df['borrowers'].mean()

    borrow_summary = [[loan_average, loan_median, collateral_average, 
                        daily_loan_average, daily_borrows_average, daily_borrower_average]]
    columns = ['loan_average', 'loan_median', 'collateral_average',
                'daily_loan_average', 'daily_borrows_average', 'daily_borrower_average']

    borrow_summary_df = pd.DataFrame(borrow_summary, columns=columns)

    supabase.update_supabase('borrow_summary_v2', borrow_summary_df)

def process_borrow_bins(df: pd.DataFrame, supabase: SupabaseClient):
    q1, q2, q3 = df["loan_amt"].quantile([0.25, 0.50, 0.75])
    loan_min, loan_max = df["loan_amt"].min(), df["loan_amt"].max()
    quartile_bins = [(loan_min - 1), q1, q2, q3, loan_max]
    quartile_labels = ["Low", "Med-Low", "Med-High", "High"]
    df["bins"] = pd.cut(df["loan_amt"], bins=quartile_bins, labels=quartile_labels, include_lowest=True)

    df_binned = df.groupby(['bins']).agg(
            borrows = ('count', 'sum'),
            borrowers = ('borrower', lambda x: x.drop_duplicates().count()),
            loan_amt =('loan_amt', 'sum'),
            loan_average = ('loan_amt', 'mean'),
            collateral_amt = ('collateral', 'sum'),
            collateral_average = ('collateral', 'mean')
    ).reset_index()

    df_binned['bin_min'] = [0, q1, q2, q3]
    df_binned['bin_max'] = [q1, q2, q3, loan_max]

    supabase.update_supabase('borrow_bins', df_binned)

def main():
    supabase = SupabaseClient()
    df_new = get_new_loans(musd)
    df_all = get_all_loans(musd)
    df_new_loans = created_merged_loans(df_new, df_all, musd)
    daily_df = process_daily_borrows(df_new_loans, supabase)
    process_weekly_borrows(daily_df, supabase)
    process_borrow_summary(df_new_loans, daily_df, supabase)
    process_borrow_bins(df_new_loans, supabase)

    print("🍄 You did it! 🍄")

if __name__ == "__main__":
    main()