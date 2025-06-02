
import pandas as pd
from datetime import datetime

from mezo.currency_utils import format_musd_currency_columns
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import load_raw_data
from mezo.clients import SupabaseClient
from scripts.get_raw_data import get_all_loans

supabase = SupabaseClient()

get_all_loans()

updated_on = datetime.today().date()
raw_loans = load_raw_data(f'{updated_on}_musd_loans.csv')

# Copy raw_loans to process data w/o polluting raw file
loans_df = raw_loans.copy()
loans_df = loans_df.sort_values(by='timestamp_', ascending=False)

# Clean the dataframe with datetime and currency formatting helpers
loans_df1 = format_datetimes(loans_df, ['timestamp_'])
loans_df2 = format_musd_currency_columns(loans_df1, ['principal', 'coll', 'stake', 'interest'])
formatted_loans = loans_df2.copy()

##################################

# Create a df with only newly opened loans
new_loans = loans_df.loc[loans_df['operation'] == 0]

# Compute data points
total_musd_borrowed = new_loans['principal'].sum() # the historical amount of MUSD taken out in loans
total_musd_loans = new_loans['borrower'].count() # the historical number of MUSD loans opened.

##################################

# sort loans_df by date desc, then drop duplicates to get only the most recent txn on an MUSD loan
loans_df_sorted = loans_df.sort_values(by='timestamp_', ascending=False)
loans_df_latest = loans_df.drop_duplicates(subset='borrower', keep='first')

# Compute data points
all_musd_borrowers = loans_df_latest['borrower'].count() # the number of users who have an open MUSD loan
current_total_musd_borrowed = loans_df_latest['principal'].sum() # the amount of MUSD currently circulating in loans
current_total_collateral = loans_df_latest['coll'].sum() # amount of BTC collateral in open loans

##################################

# Create df with only open loans
loans_without_closed_loans = loans_df_latest.loc[loans_df['operation'] != 1]

# Compute data points
current_total_musd_loans = loans_without_closed_loans['borrower'].count() # the number of currently open MUSD loans

##################################

# Create a df with only adjusted loans (including multiple adjustments from a single user)
adjusted_loans = formatted_loans.loc[formatted_loans['operation'] == 2]

# Drop duplicates and leave only the most recent adjustment per borrower
adjusted_loans_latest = loans_df_latest.loc[loans_df_latest['operation'] == 2]

# Compute data points
current_total_adjustments = adjusted_loans['borrower'].count()

##################################

# Set up a df to use for adjusted loan analysis

adjusted_loans_sorted = adjusted_loans.sort_values(by=['borrower', 'timestamp_'])
first_tx = adjusted_loans_sorted.groupby('borrower').first().reset_index()
adjusted_loans_merged = adjusted_loans_sorted.merge(first_tx[['borrower', 'principal', 'coll']], on='borrower', suffixes=('', '_initial'))

##################################

# Examine increased loans
increased_loans = adjusted_loans_merged[adjusted_loans_merged['principal'] > adjusted_loans_merged['principal_initial']].copy()

# Compute data points
total_increases = increased_loans.count()[0]
borrowers_who_increased_loans = increased_loans['borrower'].unique()
total_loans_increased = pd.DataFrame(borrowers_who_increased_loans).count()[0]


## Examine collateral changes
coll_increased = adjusted_loans_merged[adjusted_loans_merged['coll'] > adjusted_loans_merged['coll_initial']].copy()
coll_decreased = adjusted_loans_merged[adjusted_loans_merged['coll'] < adjusted_loans_merged['coll_initial']].copy()

# Compute data points
total_coll_increases = coll_increased.count()[0]
total_coll_decreases = coll_decreased.count()[0]
borrowers_who_increased_coll = coll_increased['borrower'].unique()
total_loans_with_increased_coll = pd.DataFrame(borrowers_who_increased_coll).count()[0]
borrowers_who_decreased_coll = coll_decreased['borrower'].unique()
total_loans_with_decreased_coll = pd.DataFrame(borrowers_who_decreased_coll).count()[0]


## Examine MUSD Repayments
principal_decreased = adjusted_loans_merged[adjusted_loans_merged['principal'] < adjusted_loans_merged['principal_initial']].copy()

# Compute data points
total_partial_repayments = principal_decreased.count()[0]
borrowers_who_made_repayment = principal_decreased['borrower'].unique()
total_loans_with_partial_repayments = pd.DataFrame(borrowers_who_made_repayment).count()[0]

# Examine MUSD Closed Loans/Full Repayments
closed_loans = loans_df.loc[loans_df['operation'] == 0]

# Compute data points
total_closed_loans = closed_loans['borrower'].count() 
total_musd_repaid = closed_loans['principal'].sum()
total_collateral_on_repaid_loans = closed_loans['coll'].sum()

# Summary dataframes

# dataframe with summary cumulative data
d = {
    'total_musd_borrowed' : total_musd_borrowed, 
    'total_loans' : total_musd_loans,
    'total_borrowers' : all_musd_borrowers, 
    'MUSD_in_loans' : current_total_musd_borrowed, 
    'current_collateral' : current_total_collateral, 
    'open_loans' : current_total_musd_loans
}

musd_summary = pd.DataFrame([d])
supabase.update_supabase('mainnet_musd_borrow_summary', musd_summary)


# dataframe with adjustments summary
e = {
    'total_adjustments' : current_total_adjustments, 
    'total_principal_increase' : total_increases, 
    'unique_principal_increase' : total_loans_increased,
    'total_coll_increase' : total_coll_increases,
    'unique_coll_increase' : total_loans_with_increased_coll, 
    'total_coll_decreasee' : total_coll_decreases, 
    'unique_coll_decrease' : total_loans_with_decreased_coll, 
    'total_partial_repayments' : total_partial_repayments, 
    'unique_partial_repayments' : total_loans_with_partial_repayments
}

musd_adjustments_summary = pd.DataFrame([e])
supabase.update_supabase('mainnet_musd_adjustments_summary', musd_adjustments_summary)

# dataframe with closed loan summary
f = {
    'total_closed_loans' : total_closed_loans, 
    'total_repaid' : total_musd_repaid, 
    'total_coll_on_closed_loans' : total_collateral_on_repaid_loans
}

musd_closed_loans_summary = pd.DataFrame([f])
supabase.update_supabase('mainnet_musd_closed_loans_summary', musd_closed_loans_summary)

print('✅ Run successful!')