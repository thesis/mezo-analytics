# GitHub Actions Data Processing

This directory contains GitHub Actions workflows for automated data processing.

## Workflows

### 🕕 `data-processing.yml` - Automated Processing
**Schedule:** Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)

**What it does:**
- Runs `process_bridge_data.py` to fetch and process bridge transaction data
- Runs `process_musd_data.py` to fetch and process MUSD loan data
- Uploads data to Supabase database
- Creates processing artifacts for debugging

**Features:**
- ✅ Automatic table creation with dynamic schema detection
- 🔄 Retry logic with visual progress indicators
- 📊 Processing summaries and status reports
- 🚨 Failure notifications
- 📁 Artifact uploads for debugging

### 🔧 `manual-data-processing.yml` - Manual Testing
**Trigger:** Manual workflow dispatch

**Options:**
- Choose specific script to run (both, bridge_only, musd_only)
- Enable debug mode for detailed output
- Upload processing artifacts

## Required Secrets

Configure these secrets in your GitHub repository settings:

```
SUPABASE_URL_PROD      # Production Supabase project URL
SUPABASE_KEY_PROD      # Production Supabase API key
SUPABASE_DATA_URL      # Data warehouse Supabase project URL  
SUPABASE_DATA_KEY      # Data warehouse Supabase API key
COINGECKO_KEY          # CoinGecko API key for price data
```

## Monitoring

### Success Indicators
- ✅ All jobs complete with `success` status
- 🎉 Summary shows successful processing
- 📊 Artifacts contain generated CSV files

### Failure Handling
- ❌ Failed jobs are reported in the summary
- 🔔 Workflow fails if any job fails
- 📋 Detailed logs available in job outputs
- 📁 Artifacts uploaded even on failure for debugging

## Usage

### Automatic Execution
The workflows run automatically every 6 hours. No action required.

### Manual Execution
1. Go to **Actions** tab in GitHub
2. Select **Manual Data Processing**
3. Click **Run workflow**
4. Choose options and run

### Monitoring Results
1. Check the **Actions** tab for workflow status
2. View the summary in the workflow run
3. Download artifacts if needed for debugging

## Dependencies

- Python 3.11
- Requirements from `requirements.txt`
- Access to Supabase and CoinGecko APIs

## Data Flow

```
GitHub Actions → Python Scripts → Supabase Database
     ↓                ↓               ↓
  Artifacts      Processing        Live Data
                   Logs
```

## Troubleshooting

### Common Issues
1. **API Rate Limits**: Scripts include retry logic
2. **Table Schema**: Dynamic table creation handles structure changes
3. **Network Issues**: Workflows have timeout protection

### Debug Mode
Enable debug mode in manual workflows for detailed output:
- Verbose Python execution (`python -v`)
- Detailed error tracebacks
- Extended logging output