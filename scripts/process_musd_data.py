from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import os
import requests
from mezo.currency_utils import Conversions, get_token_price
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pct_change_columns, add_cumulative_columns
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import MUSDQueries
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.test_utils import tests

# ==================================================
# helper functions
# ==================================================

@with_progress("Cleaning loan data")
def clean_loan_data(raw, sort_col, date_cols, currency_cols):
    """Clean and format loan data."""
    conversions = Conversions()
    if not ExceptionHandler.validate_dataframe(raw, "Raw loan data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col, ascending=False)
    df = format_datetimes(df, date_cols)
    df = conversions.format_token_decimals(df, amount_cols=currency_cols)
    # df = format_musd_currency_columns(df, currency_cols)
    df['count'] = 1
    return df

@with_progress("Calculating collateralization ratios")
def find_coll_ratio(df, token_id):
    """Computes the collateralization ratio"""
    conversions = Conversions()
    usd = conversions.get_token_price(token_id)
    df['coll_usd'] = df['coll'] * usd
    df['coll_ratio'] = (df['coll_usd']/df['principal']).fillna(0)
    return df

@with_progress("Filtering loan subsets")
def get_loans_subset(df, operation: int, equals):
    """Create a df with only new, adjusted, or closed loans
    0 = opened, 1 = closed, 2 = adjusted, 3 = refinanced
    note: operation = 2 also includes liquidated loans, so we have to remove those manually
    """
    df_copy = df.copy()
    df_copy['operation'] = df_copy['operation'].astype(int)
    if equals is True:
        result = df_copy.loc[df_copy['operation'] == operation]
    elif equals is False:
        result = df_copy.loc[df_copy['operation'] != operation]
    return result

@with_progress("Processing liquidation data")
def process_liquidation_data(liquidations, troves_liquidated):
    """Merge raw liquidation data from two queries"""
    liquidation_df_merged = pd.merge(
        liquidations, 
        troves_liquidated, 
        how='left', 
        on='transactionHash_'
    )

    liquidation_df_merged = liquidation_df_merged[
        ['timestamp__x', 
        'liquidatedPrincipal', 
        'liquidatedInterest', 
        'liquidatedColl', 
        'borrower',
        'transactionHash_',
        'count_x'
        ]
    ]

    liquidations_df_final = liquidation_df_merged.rename(
        columns = {
            'timestamp__x': 'timestamp_', 
            'liquidatedPrincipal': 'principal', 
            'liquidatedInterest': 'interest',
            'liquidatedColl': 'coll',
            'count_x': 'count'
        }
    )

    liquidations_final = liquidations_df_final.copy()
    liquidations_final['coll'] = liquidations_final['coll'].astype(float)

    return pd.DataFrame(liquidations_final)

@with_progress("Creating daily loan aggregations")
def create_daily_loan_data(new_loans, closed_loans, adjusted_loans, latest_loans):
    """Create daily aggregated loan data"""
    
    daily_new_loans = new_loans.groupby(['timestamp_']).agg(
        loans_opened = ('count', 'sum'),
        borrowers = ('borrower', lambda x: x.nunique()),
        principal = ('principal', 'sum'),
        collateral = ('coll', 'sum'),
        interest = ('interest', 'sum')
    ).reset_index()

    daily_closed_loans = closed_loans.groupby(['timestamp_']).agg(
        loans_closed = ('count', 'sum'),
        borrowers_who_closed = ('borrower', lambda x: x.nunique())
    ).reset_index()

    daily_new_and_closed_loans = pd.merge(daily_new_loans, daily_closed_loans, how='outer', on='timestamp_').fillna(0)
    daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']] = daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']].astype('int')      
    
    daily_adjusted_loans = adjusted_loans.groupby(['timestamp_']).agg(
        loans_adjusted = ('count', 'sum'),
        borrowers_who_adjusted = ('borrower', lambda x: x.nunique())
    ).reset_index()

    daily_loan_data = pd.merge(daily_new_and_closed_loans, daily_adjusted_loans, how='outer', on='timestamp_').fillna(0)
    daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']] = daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']].astype(int)

    daily_balances = latest_loans.groupby(['timestamp_']).agg(
        musd = ('principal', 'sum'),
        interest = ('interest', 'sum'),
        collateral = ('coll', 'sum')
    ).reset_index()

    daily_balances = daily_balances.rename(
        columns={'musd': 'net_musd', 
                 'interest': 'net_interest',
                 'collateral': 'net_coll'}
    )

    daily_loans_merged = pd.merge(daily_loan_data, daily_balances, how='outer', on='timestamp_')

    cols = {
        'timestamp_': 'date', 
        'principal': 'gross_musd', 
        'collateral': 'gross_coll', 
        'interest': 'gross_interest',
        'borrowers_who_closed': 'closers', 
        'borrowers_who_adjusted': 'adjusters'
    }

    daily_loans_merged = daily_loans_merged.rename(columns=cols)
    daily_loans_merged = daily_loans_merged.sort_values(by='date')
    daily_musd_final = add_rolling_values(daily_loans_merged, 30, ['net_musd', 'net_interest', 'net_coll']).fillna(0)
    daily_musd_final = add_cumulative_columns(daily_musd_final, ['net_musd', 'net_interest', 'net_coll'])
    daily_musd_final = add_pct_change_columns(daily_musd_final, ['net_musd', 'net_interest', 'net_coll'], 'daily').fillna(0)
    daily_musd_final = daily_musd_final.replace([float('inf'), -float('inf')], 0)
    daily_musd_final['date'] = pd.to_datetime(daily_musd_final['date']).dt.strftime('%Y-%m-%d')
    
    return daily_musd_final

def create_daily_token_data(mints, burns):
    daily_mints = mints.groupby(['timestamp_']).agg(
        mints = ('from', 'count'),
        minters = ('to', lambda x: x.nunique()),
        amt_minted = ('value', 'sum')
    ).reset_index()

    daily_burns = burns.groupby(['timestamp_']).agg(
        burns = ('from', 'count'),
        burners = ('from', lambda x: x.nunique()),
        amt_burned = ('value', 'sum')
    ).reset_index()

    daily_df = pd.merge(daily_mints, daily_burns, how='outer', on='timestamp_').fillna(0)
    daily_df = add_rolling_values(daily_df, 7, cols=['amt_minted', 'amt_burned'])
    daily_df = add_cumulative_columns(daily_df, cols=['mints', 'amt_minted', 'burns', 'amt_burned']).fillna(0)
    
    return daily_df

@with_progress("Processing loan adjustments")
def process_loan_adjustments(adjusted_loans):
    """Break down adjusted loan types for analysis"""
    adjusted_loans = adjusted_loans.sort_values(by=['borrower', 'timestamp_'])
    first_tx = adjusted_loans.groupby('borrower').first().reset_index()

    adjusted_loans_merged = adjusted_loans.merge(
        first_tx[['borrower', 'principal', 'coll']], 
        on='borrower', 
        suffixes=('', '_initial')
    )

    # Loan increases
    increased_loans = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                            > adjusted_loans_merged['principal_initial']].copy()
    increased_loans['type'] = 1

    # Collateral changes
    coll_increased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                           > adjusted_loans_merged['coll_initial']].copy()
    coll_increased['type'] = 2

    coll_decreased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                           < adjusted_loans_merged['coll_initial']].copy()
    coll_decreased['type'] = 3

    # MUSD Repayments
    principal_decreased = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                                < adjusted_loans_merged['principal_initial']].copy()
    principal_decreased['type'] = 4

    # Create final_adjusted_loans dataframe with type column
    final_adjusted_loans = pd.concat([
        increased_loans,
        coll_increased,
        coll_decreased,
        principal_decreased
    ], ignore_index=True)

    # Create composite unique identifier to handle cases where same transaction
    # appears in multiple categories (e.g., both principal and collateral increase)
    # final_adjusted_loans['composite_id'] = final_adjusted_loans['transactionHash_'] + '_' + final_adjusted_loans['type'].astype(str)

    return final_adjusted_loans

@with_progress("Fetching MUSD token data")
def fetch_musd_token_data():
    """Fetch MUSD token transfers and metadata"""
    musd_token_address = '0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186'
    conversions = Conversions()

    # Get holder data
    base_url = 'http://api.explorer.mezo.org/api/v2/tokens/'
    url = f'{base_url}{musd_token_address}/counters'
    response = requests.get(url, timeout=10)
    data = response.json()
    dat = pd.json_normalize(data)
    musd_holders = pd.DataFrame(dat)

    # Get token data
    url2 = f'{base_url}{musd_token_address}/'
    response = requests.get(url2, timeout=10)
    data2 = response.json()
    dat2 = pd.json_normalize(data2)
    musd_token_data = pd.DataFrame(dat2)

    musd_token = pd.merge(musd_token_data, musd_holders, how='cross')
    musd_token = musd_token[['circulating_market_cap', 'exchange_rate', 
                           'holders', 'total_supply', 'volume_24h', 
                           'token_holders_count', 'transfers_count']]
    musd_token['timestamp'] = datetime.now()

    # format_musd_currency_columns(musd_token, ['total_supply'])
    musd_token = conversions.format_token_decimals(musd_token, amount_cols=['total_supply'])
    musd_token = musd_token.fillna(0)
    
    return musd_token

@with_progress("Calculate liquidation risk per loan")
def add_loan_risk(df):
    df['liquidation_buffer'] = df['coll_ratio'] - 1.3

    df['risk_category'] = pd.cut(
        df['liquidation_buffer'],
        bins=[-float('inf'), 0, 0.1, 0.3, float('inf')],
        labels=['Critical', 'High', 'Medium', 'Low']
    )

    return df

@with_progress("Create risk distribution for MUSD loans")
def create_risk_distribution(df, risk_col):
    
    # Risk categories
    df['risk_category'] = pd.cut(
        df[risk_col],
        bins=[-float('inf'), 0, 0.1, 0.3, float('inf')],
        labels=['Critical', 'High', 'Medium', 'Low']
    )
    
    # Distribution analysis
    risk_distribution = df.groupby('risk_category', observed=False).agg({
        'principal': ['count', 'sum'],
        'coll': 'sum',
        'liquidation_buffer': 'mean'
    }).reset_index()

    risk_distribution.columns = ['_'.join(col).strip() 
                                 for col in risk_distribution.columns.values]

    return risk_distribution

def main(test_mode=False, sample_size=False, skip_bigquery=False):
    ProgressIndicators.print_header("MUSD DATA PROCESSING PIPELINE")

    conversions = Conversions()

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        if sample_size:
            print(f"{'Using sample size: ' + str(sample_size):^60}")
        if skip_bigquery:
            print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    try:

        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        COINGECKO_KEY = os.getenv('COINGECKO_KEY')
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        if not skip_bigquery:
            ProgressIndicators.print_step("Initializing BigQuery", "start")
            bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
            ProgressIndicators.print_step("BigQuery initialized", "success")

    # ==================================================
    # fetch raw data + upload to bigquery
    # ==================================================
        subgraph_data = [
            ('raw_loans', 'loans', SubgraphClient.BORROWER_OPS_SUBGRAPH, MUSDQueries.GET_LOANS, 'troveUpdateds'),
            ('raw_liquidations', 'liquidations', SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, MUSDQueries.GET_MUSD_LIQUIDATIONS, 'liquidations'),
            ('raw_troves_liquidated', 'liquidated troves', SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, MUSDQueries.GET_LIQUIDATED_TROVES, 'troveLiquidateds'),
            ('raw_redemptions', 'redemptions', SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, MUSDQueries.GET_REDEMPTIONS, 'redemptions'),
            ('raw_fees', 'borrow fees', SubgraphClient.BORROWER_OPS_SUBGRAPH, MUSDQueries.GET_BORROW_FEES, 'borrowingFeePaids'),
            ('raw_mints', 'MUSD mints', SubgraphClient.MUSD_TOKEN_SUBGRAPH, MUSDQueries.GET_MUSD_MINTS, 'transfers'),
            ('raw_burns', 'MUSD burns', SubgraphClient.MUSD_TOKEN_SUBGRAPH, MUSDQueries.GET_MUSD_BURNS, 'transfers')
        ]

        data_results = {}

        for var_name, display_name, subgraph, query, query_name in subgraph_data:
            ProgressIndicators.print_step(f"Fetching {display_name} data", "start")
            data_results[var_name] = SubgraphClient.get_subgraph_data(subgraph, query, query_name)
            ProgressIndicators.print_step(f"Loaded {len(data_results[var_name])} rows of {display_name}", "success")

        raw_loans = data_results['raw_loans']
        raw_liquidations = data_results['raw_liquidations']
        raw_troves_liquidated = data_results['raw_troves_liquidated']
        raw_redemptions = data_results['raw_redemptions']
        raw_fees = data_results['raw_fees']
        raw_mints = data_results['raw_mints']
        raw_burns = data_results['raw_burns']

        ProgressIndicators.print_step("Raw data fetched successfully", "success")
        
        ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")
        raw_datasets_to_upload = [
            (raw_loans, 'musd_loans_raw', 'transactionHash_'),
            (raw_liquidations, 'musd_liquidations_raw', 'transactionHash_'),
            (raw_troves_liquidated, 'musd_troves_liquidated_raw', 'transactionHash_'),
            (raw_redemptions, 'musd_redemptions_raw', 'transactionHash_'),
            (raw_fees, 'musd_fees_raw', 'transactionHash_'),
            (raw_mints, 'musd_mints_raw', 'transactionHash_'),
            (raw_burns, 'musd_burns_raw', 'transactionHash_')
        ]

        for dataset, table_name, id_column in raw_datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                dataset['transactionHash_'] = dataset['transactionHash_'].astype(str)
                bq.update_table(dataset, 'raw_data', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # ==================================================
        # Clean and process loan data + upload to bigquery
        # ==================================================
        
        loans = clean_loan_data(
            raw_loans,
            sort_col='timestamp_', 
            date_cols=['timestamp_'], 
            currency_cols=['principal', 'coll', 'stake', 'interest']
        )
        loans = find_coll_ratio(loans, 'bitcoin')

        liquidations = clean_loan_data(
            raw_liquidations,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['liquidatedPrincipal', 'liquidatedInterest', 'liquidatedColl']
        )
        troves_liquidated = clean_loan_data(
            raw_troves_liquidated,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['debt', 'coll']
        )
        liquidations_final = process_liquidation_data(liquidations, troves_liquidated)

        redemptions = clean_loan_data(
            raw_redemptions,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['actualAmount', 'attemptedAmount', 'collateralFee', 'collateralSent']
        )
        
        mints = clean_loan_data(
            raw_mints,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['value']
        )
        burns = clean_loan_data(
            raw_burns,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['value']
        )        

        # ==================================================
        # Create subsets of loans by type of txn + upload to bigquery
        # ==================================================

        new_loans = get_loans_subset(loans, 0, True)
        closed_loans = get_loans_subset(loans, 1, True)
        adjusted_loans = get_loans_subset(loans, 2, True)
        refinanced_loans = get_loans_subset(loans, 3, True)

        # remove liquidations from adjusted loans
        liquidated_borrowers = liquidations_final['borrower'].unique()
        adjusted_loans = adjusted_loans[~adjusted_loans['borrower'].isin(liquidated_borrowers)]

        latest_loans = loans.drop_duplicates(subset='borrower', keep='first')
        latest_open_loans = get_loans_subset(latest_loans, 1, False) # remove closed loans
        latest_open_loans = latest_open_loans[~latest_open_loans['borrower'].isin(liquidated_borrowers)] # remove liquidated loans

        # calculate loan risk category
        latest_open_loans = add_loan_risk(latest_open_loans)

        final_adjusted_loans = process_loan_adjustments(adjusted_loans)

        ProgressIndicators.print_step("Uploading loan subset data to BigQuery", "start")

        datasets_to_upload = [
            (loans, 'all_loans_clean', 'transactionHash_'),
            (new_loans, 'new_loans_clean', 'transactionHash_'),
            (closed_loans, 'closed_loans_clean', 'transactionHash_'),
            (latest_open_loans, 'open_loans_clean', 'transactionHash_'),
            (final_adjusted_loans, 'adjusted_loans_clean', 'transactionHash_'),
            (liquidations_final, 'liquidated_loans_clean', 'transactionHash_'),
            (refinanced_loans, 'refinanced_loans_clean', 'transactionHash_'),
            (redemptions, 'redemptions_clean', 'transactionHash_'),
            (mints, 'musd_mints_clean', 'transactionHash_'),
            (burns, 'musd_burns_clean', 'transactionHash_')
        ]

        for dataset, table_name, id_column in datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'staging', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # ==================================================
        # create daily and aggregated datasets + upload to bigquery
        # ==================================================

        final_daily_musd = create_daily_loan_data(new_loans, closed_loans, adjusted_loans, latest_loans)
        daily_mints_and_burns = create_daily_token_data(mints, burns)
        risk_distribution = create_risk_distribution(latest_open_loans, 'liquidation_buffer')
        btc_price = conversions.get_token_price('bitcoin')
        musd_token = fetch_musd_token_data()

        ProgressIndicators.print_step("Uploading daily loans data to BigQuery", "start")
        if final_daily_musd is not None and len(final_daily_musd) > 0:
            bq.update_table(final_daily_musd, 'staging', 'daily_loans_clean', 'date')
            ProgressIndicators.print_step("Uploaded final_daily_musd to BigQuery", "success")

        ProgressIndicators.print_step("Uploading daily mint and burn data to BigQuery", "start")
        if daily_mints_and_burns is not None and len(daily_mints_and_burns) > 0:
            bq.update_table(daily_mints_and_burns, 'marts', 'daily_mints_and_burns', 'timestamp_')
            ProgressIndicators.print_step("Uploaded daily_mints_and_burns to BigQuery", "success")
        
        ProgressIndicators.print_step("Uploading token data to BigQuery", "start")
        if musd_token is not None and len(musd_token) > 0:
            bq.update_table(musd_token, 'marts', 'm_musd_token', 'timestamp')
            ProgressIndicators.print_step("Uploaded risk distribution to BigQuery", "success")
        
        ProgressIndicators.print_step("Uploading risk distribution to BigQuery", "start")
        if risk_distribution is not None and len(risk_distribution) > 0:
            bq.upsert_table_by_id(risk_distribution, 'marts', 'risk_distribution', 'risk_category_')
            ProgressIndicators.print_step("Uploaded risk distribution to BigQuery", "success")

        # ==================================================
        # calculate and display summary statistics
        # ==================================================

        ProgressIndicators.print_step("Calculating summary statistics", "start")

        all_time_musd_borrowed = new_loans['principal'].sum()
        all_time_musd_loans = new_loans['count'].sum()
        all_time_musd_borrowers = new_loans['borrower'].nunique()
        all_time_closed_loans = closed_loans['count'].sum()
        all_time_adjustments = adjusted_loans['count'].sum()

        open_loans = latest_open_loans['count'].sum()
        liquidated_loans = liquidations_final['count'].sum()
        interest_liquidated = liquidations_final['interest'].sum()
        coll_liquidated = liquidations_final['coll'].sum()

        system_coll = latest_open_loans['coll'].sum()
        system_debt = latest_open_loans['principal'].sum() + latest_open_loans['interest'].sum()
        TCR = ((system_coll * btc_price) / system_debt) * 100

        musd_summary = pd.DataFrame([{
            'all_time_musd_borrowed': all_time_musd_borrowed, 
            'all_time_loans': all_time_musd_loans,
            'all_time_borrowers': all_time_musd_borrowers, 
            'system_debt': system_debt, 
            'system_coll': system_coll,
            'TCR': TCR, 
            'open_loans': open_loans,
            'closed_loans': all_time_closed_loans,
            'liquidated_loans': liquidated_loans,
            'interest_liquidated': interest_liquidated,
            'collateral_liquidated': coll_liquidated
        }])
        ProgressIndicators.print_step("Summary statistics calculated", "success")

        ProgressIndicators.print_summary_box(
            f"💰 MUSD LOAN SUMMARY STATISTICS 💰",
            {
                "BTC Price": btc_price,
                "Total MUSD Borrowed": all_time_musd_borrowed,
                "Total Loans": all_time_musd_loans,
                "Unique Borrowers": all_time_musd_borrowers,
                "Open Loans": open_loans,
                "System TCR": f"{TCR:.2f}%",
                "System Debt": system_debt
            }
        )

        ProgressIndicators.print_header(f"🚀 MUSD PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return {
            'daily_musd_data': final_daily_musd,
            'summary': musd_summary,
            'token_data': musd_token
        }
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise

if __name__ == "__main__":
    results = main()

    # results = tests.quick_test(sample_size=500)
    # tests.inspect_data(results)
    # tests.save_test_outputs(results)