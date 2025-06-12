# Mezo Analytics Mainnet

A comprehensive data processing and analytics pipeline for Mezo mainnet data, including bridge transactions, MUSD lending, and collateralization analysis.

## Overview

This repository provides automated data collection, processing, and analysis tools for Mezo's mainnet operations. It fetches data from multiple sources including subgraphs, APIs, and Mezo's blockchain explorer, processes it, and stores results in Supabase.

## Data Sources

- **Bridge Transactions**: Cross-chain bridge activity via Mezo Bridge subgraph
- **MUSD Lending**: Loan origination, adjustments, and liquidations via MUSD subgraphs
- **Token Prices**: Real-time pricing data from CoinGecko API
- **MUSD Token Data**: On-chain transfer and holder data from Mezo Explorer API

## Architecture

```
Data Sources → Processing Scripts → Supabase Database → Analytics/Visualization
     ↓               ↓                    ↓                     ↓
  Subgraphs     Python Pipeline      Live Tables           Notebooks
  APIs          Visual Progress       Auto-Schema          Dashboards
  Explorer      Error Handling       Time-Series          Reports
```

## Project Structure

```
mezo-analytics/
├── .github/workflows/          # GitHub Actions for automation
│   ├── data-processing.yml     # Main cron job (every 6 hours)
│   └── manual-data-processing.yml  # Manual trigger workflow
├── mezo/                       # Core library modules
│   ├── clients.py             # Database and API clients
│   ├── currency_config.py     # Token mappings and configurations
│   ├── currency_utils.py      # Price fetching and formatting
│   ├── data_utils.py         # Data processing utilities
│   ├── datetime_utils.py     # Date/time formatting
│   ├── queries.py            # GraphQL queries
│   └── visual_utils.py       # Progress indicators and UI
├── scripts/                   # Main processing scripts
│   ├── get_raw_data.py       # Data fetching functions
│   ├── process_bridge_data.py # Bridge transaction processing
│   ├── process_musd_data.py  # MUSD lending data processing
│   └── archive/              # Legacy scripts
├── tests/                    # Test and debug utilities
├── notebooks/               # Jupyter analysis notebooks
├── data/                   # Local data storage
└── requirements.txt        # Python dependencies
```

## Setup

### Prerequisites

- Python 3.13
- Supabase account
- CoinGecko API key

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd mezo-analytics-mainnet
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**:
   Create a `.env` file with:
   ```env
   SUPABASE_URL_PROD=your_mezo_portal_prod_supabase_url
   SUPABASE_KEY_PROD=your_mezo_portal_prod_supabase_key
   SUPABASE_DATA_URL=your_data_analytics_supabase_url
   SUPABASE_DATA_KEY=your_data_analytics_supabase_key
   COINGECKO_KEY=your_coingecko_api_key
   ```

## Usage

### Automated Processing

The repository includes GitHub Actions workflows that run automatically:

- **Every 6 hours**: Processes both bridge and MUSD data
- **Manual trigger**: On-demand processing via GitHub Actions UI

### Manual Processing

Run individual scripts locally:

```bash
# Process bridge transaction data
python scripts/process_bridge_data.py

# Process MUSD lending data
python scripts/process_musd_data.py
```

## Key Features

### **Comprehensive Data Processing**
- **Bridge Analytics**: Daily aggregations by token with USD values
- **MUSD Metrics**: Loan lifecycle, liquidations, and system health
- **System Health**: Collateralization ratios and risk metrics

### **Advanced Analytics**
- **Time Series**: Daily, rolling, and cumulative metrics
- **Cohort Analysis**: User behavior and loan patterns
- **Risk Assessment**: Collateralization and liquidation analysis
- **Token Distribution**: Cross-chain asset flow tracking

## Data Outputs

### Bridge Data Supabase Tables
- `mainnet_daily_bridge_data` - Daily bridge volume by token
- `mainnet_bridge_by_token` - Token-level bridge summaries
- `mainnet_bridge_summary` - Overall bridge statistics

### MUSD Data Supabase Tables
- `mainnet_musd_daily` - Daily lending activity and balances
- `mainnet_musd_borrow_summary` - Cumulative borrowing metrics
- `mainnet_musd_system_health` - Collateralization and stability
- `mainnet_musd_averages` - Loan size and ratio statistics
- `mainnet_musd_token_summary` - Token holder and transfer data

## Configuration

### Core Library Files (`mezo/`)

#### **Configuration Files**
- **`currency_config.py`**: Token mappings and configurations
  - `TOKEN_MAP`: Contract address → Symbol mapping
  - `TOKEN_TYPE_MAP`: Symbol → Category (bitcoin, stablecoin, ethereum)
  - `TOKENS_ID_MAP`: Symbol → CoinGecko ID for price fetching

- **`subgraph_config.py`**: Centralized subgraph endpoint definitions
  - Portal, Market, Bridge, Borrower Operations subgraphs
  - MUSD Token, Stability Pool, Trove Manager endpoints

#### **Data Processing Utilities**
- **`currency_utils.py`**: Currency formatting and price fetching
  - `format_currency_columns()`: Handles token decimal conversions (1e6, 1e8, 1e18)
  - `get_token_prices()`: Fetches USD prices from CoinGecko API
  - `replace_token_labels()`: Maps contract addresses to symbols

- **`datetime_utils.py`**: Date and time processing
  - `convert_unix_to_datetime()`: Unix timestamp conversion with timezone handling
  - `format_datetimes()`: Standardizes date columns to YYYY-MM-DD format
  - `groupby_date()`: Date-based aggregation helper

- **`data_utils.py`**: Analytics and transformation utilities
  - `add_cumulative_columns()`: Creates cumulative sum columns
  - `add_pct_change_columns()`: Calculates percentage change metrics
  - `add_rolling_values()`: Generates rolling window averages

#### **Query Definitions**
- **`queries.py`**: GraphQL query templates
  - `MUSDQueries`: Loan data, liquidations, collateral snapshots
  - `BridgeQueries`: Bridge transactions, deposits, withdrawals

#### **Database Client**
- **`clients.py`**: Database and API communication
  - `SupabaseClient`: Dual-database architecture (production + data warehouse)
  - `SubgraphClient`: GraphQL query execution with pagination
  - `APIClient`: Generic HTTP client for REST APIs

#### **User Interface**
- **`visual_utils.py`**: Progress indicators and error handling
  - `ProgressIndicators`: Visual symbols (✅❌⚠️) and progress bars
  - `ExceptionHandler`: Retry logic and safe execution with visual feedback
  - Decorators: `@with_progress()` and `@safe_operation()`

## Development

### Adding New Data Sources
1. Create fetch functions in `scripts/get_raw_data.py`
2. Add processing logic following existing patterns and naming conventions
3. Update configurations in `mezo/currency_config.py`
4. Add tests in `tests/`

### Contributing
1. Follow existing code patterns and visual indicators
2. Add comprehensive error handling
3. Include progress indicators for long-running operations
4. Write tests for new functionality
5. Update documentation

## Analysis Examples

The `notebooks/` directory contains analysis examples:
- Bridge transaction flow analysis
- MUSD lending market dynamics
- Collateralization ratio distributions
- Cross-chain asset migration patterns

---

## Quick Start

1. **Set up environment variables** in `.env`
2. **Run initial processing**:
   ```bash
   python scripts/process_bridge_data.py
   ```
3. **Check Supabase** for generated tables and data
4. **Enable GitHub Actions** for automated processing
5. **Explore notebooks** for analysis examples
