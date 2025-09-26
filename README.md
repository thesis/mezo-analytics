# Mezo Analytics

A comprehensive data analytics pipeline for the Mezo protocol, processing blockchain data from subgraphs, smart contracts, and block explorer APIs to generate insights and dashboards. It exports raw and transformed data to Mezo's BigQuery warehouse.

## Overview

This repository contains scripts and utilities for:
- Fetching and processing data from Mezo mainnet subgraphs
- Querying smart contracts via Web3.py (note: these queries are performed directly in Hex)
- Retrieving transaction data from block explorer APIs
- Processing all major transactions on Mezo mainnet, including providing liquidity, swapping, vaulting assets, borrowing, and purchasing items in the MUSD market.

## Architecture

### Project Structure

```
mezo-analytics/
├── .env.example                    # Environment variables template
├── .gitignore                      # Git ignore rules
├── README.md                       # Project documentation
├── requirements.txt                # Python dependencies
├── setup.py                        # Package setup configuration
│
├── mezo/                          # Core library modules
│   ├── __init__.py               # Package initialization
│   ├── clients.py                # API clients (SubgraphClient, Web3Client, BigQueryClient)
│   ├── queries.py                # GraphQL queries for subgraphs
│   ├── currency_config.py        # Token mappings and currency configurations
│   ├── currency_utils.py         # Currency conversion and formatting utilities
│   ├── datetime_utils.py         # Date/time formatting and conversion
│   ├── data_utils.py             # Data processing helper functions
│   ├── visual_utils.py           # Progress indicators and error handling
│   └── smart_contracts/          # Contract ABIs and configurations
│       ├── ActivePool.json
│       ├── BorrowerOperations.json
│       ├── MezoBridge.json
│       ├── PoolFactory.json
│       ├── PriceFeed.json
│       ├── Router.json
│       ├── StabilityPool.json
│       └── TroveManager.json
│
├── scripts/                       # Data processing pipelines
│   ├── __init__.py
│   │
│   ├── # Main Processing Scripts
│   ├── process_pools_data.py     # Liquidity pool metrics and TVL
│   ├── process_musd_data.py      # MUSD loan and collateral processing
│   ├── process_swaps_data.py     # Swap volume and fee analytics
│   ├── process_market_data.py    # Market transactions and donations
│   ├── process_bridge_data.py    # Bridge in/out transaction processing
│   ├── process_vaults_data.py    # Vault deposits and withdrawals
│   ├── process_dapp_data.py      # DApp transaction processing
│   │
│   ├── # Data Fetching Scripts
│   ├── fetch_market_transactions.py  # Fetch from block explorer API
│   ├── fetch_mezo_users.py          # User data collection
│   ├── get_raw_data.py             # Raw subgraph data fetching
│   │
│   ├── # Utility Scripts
│   ├── update_requirements.py    # Auto-update requirements.txt
│   ├── update_all_data.py       # Orchestrate all data updates
│   │
│   └── archive/                  # Deprecated/archived scripts
│       ├── get_raw_autobridge_data.py
│       └── get_raw_loan_data.py
│
├── notebooks/                     # Jupyter notebooks for analysis
│   ├── exploratory_analysis.ipynb
│   ├── pool_metrics_analysis.ipynb
│   └── user_behavior_analysis.ipynb
│
├── tests/                         # Test suite
│   ├── __init__.py
│   ├── test_clients.py
│   ├── test_currency_utils.py
│   ├── test_data_processing.py
│   └── test_queries.py
│
└── docs/                          # Additional documentation
    ├── api_reference.md
    ├── data_dictionary.md
    └── deployment_guide.md
```

### Data Flow Architecture

### Data Flow Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Subgraph APIs  │     │ Smart Contracts │     │  Block Explorer │
│   (Goldsky)     │     │   (Web3.py)     │     │      API        │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                        │
         ▼                       ▼                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ SubgraphClient  │     │   Web3Client    │     │  API Requests   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                        │
         └───────────────────────┼────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Raw Data Collection   │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │ Data Processing Scripts │
                    │  • process_pools_data   │
                    │  • process_musd_data    │
                    │  • process_swaps_data   │
                    │  • process_market_data  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Data Transformation   │
                    │  • Cleaning & Validation│
                    │  • Currency Conversion  │
                    │  • Metrics Calculation  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    BigQuery Storage     │
                    ├─────────────────────────┤
                    │  • raw_data/            │
                    │  • intermediate/        │
                    │  • marts/               │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │     Hex Dashboards      │
                    │  • Executive KPIs       │
                    │  • Pool Analytics       │
                    │  • Risk Metrics         │
                    └─────────────────────────┘
```

### Data Sources

#### 1. Subgraph APIs (Goldsky)

All subgraphs are hosted on Goldsky and accessed via GraphQL queries:

| Subgraph | URL | Purpose |
|----------|-----|---------|
| **Mezo Portal** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-portal-mainnet/1.0.0/gn` | Auto-bridge transactions from Mainnet |
| **Mezo Bridge** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-bridge-mainnet/1.0.0/gn` | Bridge transactions into Mezo |
| **Bridge Out** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/bridge-out-mezo/1.0.0/gn` | Bridge transactions out of Mezo |
| **Borrower Operations** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/borrower-operations-mezo/1.0.0/gn` | MUSD loan operations (open/close/adjust troves) |
| **MUSD Token** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-token/1.0.0/gn` | MUSD token transfers and mints |
| **MUSD Market** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/market-mezo/1.0.0/gn` | MUSD market donations and purchases |
| **Stability Pool** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-stability-pool/1.0.0/gn` | MUSD stability pool operations |
| **Trove Manager** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-trove-manager/1.0.0/gn` | Trove liquidations and redemptions |
| **MUSD Pools** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-pools-mezo/1.0.0/gn` | MUSD liquidity pools and swaps |
| **Tigris Pools** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/tigris-pools-mezo/1.0.0/gn` | Tigris protocol pools |
| **August Vaults** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-vaults-mezo/1.0.0/gn` | August protocol vaults |
| **Wormhole** | `https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/wormhole-bridge-mezo/1.0.0/gn` | Wormhole bridge operations |

#### 2. Smart Contracts (Web3.py)

The following contracts are queried directly using Web3.py:

| Contract | Address | Methods Called |
|----------|---------|----------------|
| **BorrowerOperations** | `0x5fE95B9Bb60bE973cb6c90Cd07eC69b7E88fafE2` | ``borrowingRate()`, `refinancingFeePercentage()` |
| **TroveManager** | `0x94AfB503dBca74aC3E4929BACEeDfCe19B93c193` | `liquidateTroves()`, `MUSD_GAS_COMPENSATION()`, `getCurrentICR()` |
| **PCV** | `0x391EcC7ffEFc48cff41D0F2Bb36e38b82180B993` | `debtToPay()` | 

**RPC Endpoint:** `https://mainnet.mezo.public.validationcloud.io/`

#### 3. Block Explorer API

**Base URL:** `https://api.explorer.mezo.org/api/v2/`

**Endpoints Used:**
- `/addresses/{address}/transactions` - Fetch transactions for specific contracts
- `/tokens/{token_address}/transfers` - Token transfer history
- `/tokens/{token_address}/counters` - Token holder statistics
- `/tokens/{token_address}/` - Token metadata and market data

**Contracts called via Block Explorer:**
- MUSD token: `0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186`
- Pools Router: `0x16A76d3cd3C1e3CE843C6680d6B37E9116b5C706`
- TroveManager: `0x94afb503dbca74ac3e4929bacededfce19b93c193`
- August MUSD Vault: `0x221B2D9aD7B994861Af3f4c8A80c86C4aa86Bf53`

## Data Processing Pipeline

### 1. Raw Data Collection
```python
# Subgraph queries via SubgraphClient
SubgraphClient.fetch_subgraph_data(query, method)

# Smart contract queries via Web3Client
Web3Client.load_contract().functions.method_name().call()

# Block explorer API
requests.get(f"https://api.explorer.mezo.org/api/v2/{endpoint}")
```

### 2. Data Processing Modules

#### Pool Data Processing (`process_pools_data.py`)
- **Metrics Calculated:**
  - TVL (Total Value Locked) with growth rates
  - Trading volume and efficiency ratios
  - Fee revenue analytics
  - User engagement metrics
  - Pool health indicators with composite scores
  - Net flows (deposits - withdrawals)

#### MUSD Data Processing (`process_musd_data.py`)
- **Metrics Calculated:**
  - Loan originations and closures
  - Collateralization ratios
  - Liquidation risk categories
  - Borrowing fees and interest rates
  - Stability pool deposits
  - Redemption volumes

#### Swap Data Processing (`process_swaps_data.py`)
- **Metrics Calculated:**
  - Swap volumes by pool
  - Price impact analysis
  - Trading fee revenue
  - Token pair liquidity
  - Slippage metrics

#### Market Data Processing (`process_market_data.py`)
- **Metrics Calculated:**
  - Purchase volumes by product
  - Donation amounts
  - Transaction fees (gas costs)
  - User participation rates

### 3. Data Storage

All processed data is stored in BigQuery with the following structure:

```
mezo-portal-data/
├── raw_data/           # Raw subgraph and API data
├── staging/            # Cleaned and normalized data
├── intermediate/       # Transformed data
└── marts/             # Aggregated metrics and analytics
```

## Key Classes and Methods

### SubgraphClient
```python
class SubgraphClient:
    def __init__(self, url, headers)
    def fetch_subgraph_data(query, method) -> List[Dict]
    def get_subgraph_data(subgraph_url, query, query_key) -> pd.DataFrame
```

### Web3Client
```python
class Web3Client:
    def __init__(self, contract_name: str)
    def load_abi() -> Dict
    def load_contract() -> Contract
```

### BigQueryClient
```python
class BigQueryClient:
    def __init__(self, key: str = None, project_id: str = None)
    def create_dataset(self, dataset_id: str, location: str = "US")
    def create_table(self, df: pd.DataFrame, dataset_id: str, table_id: str)
    def table_exists(self, dataset_id: str, table_id: str) -> bool
    def update_table(self, df: pd.DataFrame, dataset_id: str, table_id: str, id_column: str)
    def upsert_table_by_id(self, df: pd.DataFrame, dataset_id: str, table_id: str, id_column: str)
```

## Environment Variables

Required environment variables (in `.env` file):
```
# API Keys
COINGECKO_KEY=your_coingecko_api_key
GOOGLE_CLOUD_KEY=google_cloud_key_for_prod_warehouse
GOOGLE_CLOUD_KEY_DEV=google_cloud_key_for_dev_warehouse

# Database Connections
SUPABASE_URL_PROD=your_supabase_url
SUPABASE_KEY_PROD=your_supabase_key
SUPABASE_DATA_URL=your_data_url
SUPABASE_DATA_KEY=your_data_key

# Web3 Configuration
MEZO_RPC_URL=https://mainnet.mezo.public.validationcloud.io/
```

## Dashboard Metrics

The processed data feeds into Hex dashboards with the following key metrics:

### Executive Summary KPIs
- Total Value Locked (TVL)
- 24h Trading Volume
- Total Users
- Protocol Revenue

### Pool Performance
- TVL by Pool (time series)
- Volume/TVL Efficiency Ratio
- Fee APY by Pool
- User Growth Trends

### Risk Analytics
- Collateralization Ratios
- Liquidation Risk Distribution
- Stability Pool Coverage
- Bad Debt Tracking

## Installation

```bash
# Clone the repository
git clone https://github.com/your-org/mezo-analytics.git
cd mezo-analytics

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials
```

## Usage

### Running Data Pipelines

```bash
# Process pool data
python scripts/process_pools_data.py

# Process MUSD loan and token
python scripts/process_musd_data.py

# Process swap data
python scripts/process_swaps_data.py

# Process bridging data (native)
python scripts/process_bridge_data.py

# Process vaulting data
python scripts/process_vaults_data

# Process ecosystem dapp data
python scripts/process_dapp_data

# Process MUSD market data
python scripts/process_market_data
```

### Scheduling Updates

Uses Github Actions to run updates periodically:
```yml
  schedule:
    # Run every 6 hours (at 00:00, 06:00, 12:00, 18:00 UTC)
    - cron: '0 */6 * * *'
```

## Data Quality

### Error Handling
- Null value checking with fallback defaults
- Retry logic for API calls with exponential backoff
- Data validation before uploads
- Comprehensive logging of processing steps

### Data Integrity
- Deduplication by transaction hash
- Timestamp normalization to UTC
- Currency formatting consistency
- Cross-validation between data sources