#!/usr/bin/env python3
"""
Comprehensive data engineering tests for refactor_pools_data.py
Tests data validation, processing logic, edge cases, and error handling
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from dataclasses import asdict
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.refactor_pools_data import PoolDataProcessor, PoolMetrics
from mezo.clients import BigQueryClient


class TestPoolDataProcessor:
    """Test suite for PoolDataProcessor class"""

    @pytest.fixture
    def mock_bq_client(self):
        """Mock BigQuery client for testing"""
        return Mock(spec=BigQueryClient)

    @pytest.fixture
    def processor(self, mock_bq_client):
        """Create PoolDataProcessor instance with mock client"""
        return PoolDataProcessor(mock_bq_client)

    @pytest.fixture
    def sample_deposit_data(self):
        """Sample deposit transaction data"""
        return pd.DataFrame({
            'contractId_': ['0x123', '0x456', '0x123'],
            'timestamp_': [1640995200, 1641081600, 1641168000],  # 2022-01-01, 2022-01-02, 2022-01-03
            'amount0': ['1000000000', '500000000', '750000000'],  # Raw amounts
            'amount1': ['2000000000', '1000000000', '1500000000'],
            'sender': ['0xuser1', '0xuser2', '0xuser1']
        })

    @pytest.fixture
    def sample_withdrawal_data(self):
        """Sample withdrawal transaction data"""
        return pd.DataFrame({
            'contractId_': ['0x123', '0x456'],
            'timestamp_': [1641254400, 1641340800],  # 2022-01-04, 2022-01-05
            'amount0': ['500000000', '250000000'],
            'amount1': ['1000000000', '500000000'],
            'sender': ['0xuser1', '0xuser3']
        })

    @pytest.fixture
    def sample_volume_data(self):
        """Sample volume data with nested pool structure"""
        return pd.DataFrame({
            'timestamp': [1640995200, 1641081600],
            'pool': [
                {'name': 'USDC/USDT', 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'USDT'}},
                {'name': 'BTC/USDC', 'token0': {'symbol': 'BTC'}, 'token1': {'symbol': 'USDC'}}
            ],
            'totalVolume0': ['10000000000', '5000000000'],
            'totalVolume1': ['10000000000', '5000000000']
        })

    @pytest.fixture
    def sample_fee_data(self):
        """Sample fee data"""
        return pd.DataFrame({
            'timestamp': [1640995200, 1641081600],
            'pool': [
                {'name': 'USDC/USDT', 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'USDT'}},
                {'name': 'BTC/USDC', 'token0': {'symbol': 'BTC'}, 'token1': {'symbol': 'USDC'}}
            ],
            'totalFees0': ['1000000', '500000'],
            'totalFees1': ['1000000', '500000']
        })


class TestDataFetching:
    """Test data fetching functionality"""

    @patch('scripts.refactor_pools_data.SubgraphClient.get_subgraph_data')
    def test_fetch_pool_data_success(self, mock_get_data, processor):
        """Test successful data fetching"""
        # Mock return values for each data source
        mock_get_data.side_effect = [
            [{'id': '1', 'amount0': '1000'}],  # deposits
            [{'id': '2', 'amount0': '500'}],   # withdrawals
            [{'id': '3', 'volume': '2000'}],   # volume
            [{'id': '4', 'fees': '100'}]       # fees
        ]

        result = processor.fetch_pool_data()

        assert 'deposits' in result
        assert 'withdrawals' in result
        assert 'volume' in result
        assert 'fees' in result
        assert len(result['deposits']) == 1
        assert mock_get_data.call_count == 4

    @patch('scripts.refactor_pools_data.SubgraphClient.get_subgraph_data')
    def test_fetch_pool_data_empty_response(self, mock_get_data, processor):
        """Test handling of empty responses"""
        mock_get_data.return_value = []

        result = processor.fetch_pool_data()

        assert all(len(data) == 0 for data in result.values())

    @patch('scripts.refactor_pools_data.SubgraphClient.get_subgraph_data')
    def test_fetch_pool_data_exception(self, mock_get_data, processor):
        """Test handling of exceptions during data fetch"""
        mock_get_data.side_effect = Exception("Network error")

        with pytest.raises(Exception):
            processor.fetch_pool_data()


class TestTransactionProcessing:
    """Test transaction processing functionality"""

    @patch('scripts.refactor_pools_data.format_datetimes')
    @patch('scripts.refactor_pools_data.format_pool_token_columns')
    @patch('scripts.refactor_pools_data.add_pool_usd_conversions')
    def test_clean_transaction_data(self, mock_usd_conv, mock_format_tokens,
                                  mock_format_dates, processor, sample_deposit_data):
        """Test transaction data cleaning"""
        # Mock return values
        mock_format_dates.return_value = sample_deposit_data.copy()
        mock_format_tokens.return_value = sample_deposit_data.copy()
        mock_usd_conv.return_value = sample_deposit_data.copy()

        result = processor._clean_transaction_data(sample_deposit_data, 'deposit')

        assert 'transaction_type' in result.columns
        assert result['transaction_type'].iloc[0] == 'deposit'
        mock_format_dates.assert_called_once()
        mock_format_tokens.assert_called_once()
        mock_usd_conv.assert_called_once()

    def test_process_transactions_integration(self, processor, sample_deposit_data,
                                            sample_withdrawal_data):
        """Test full transaction processing integration"""
        with patch.object(processor, '_clean_transaction_data') as mock_clean:
            # Mock cleaned data with required columns
            cleaned_deposits = sample_deposit_data.copy()
            cleaned_deposits['transaction_type'] = 'deposit'
            cleaned_deposits['timestamp_'] = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03'])

            cleaned_withdrawals = sample_withdrawal_data.copy()
            cleaned_withdrawals['transaction_type'] = 'withdrawal'
            cleaned_withdrawals['timestamp_'] = pd.to_datetime(['2022-01-04', '2022-01-05'])

            mock_clean.side_effect = [cleaned_deposits, cleaned_withdrawals]

            deposits, withdrawals, all_txns = processor.process_transactions(
                sample_deposit_data, sample_withdrawal_data
            )

            assert len(deposits) == 3
            assert len(withdrawals) == 2
            assert len(all_txns) == 5
            assert all_txns['timestamp_'].is_monotonic_increasing


class TestTVLCalculations:
    """Test TVL calculation functionality"""

    def test_calculate_tvl_metrics_basic(self, processor):
        """Test basic TVL calculation"""
        # Create sample data with required columns
        deposits = pd.DataFrame({
            'pool': ['Pool1', 'Pool1', 'Pool2'],
            'timestamp_': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-01']),
            'transaction_value_usd': [1000, 500, 2000],
            'transaction_type': ['deposit', 'deposit', 'deposit']
        })

        withdrawals = pd.DataFrame({
            'pool': ['Pool1', 'Pool2'],
            'timestamp_': pd.to_datetime(['2022-01-03', '2022-01-02']),
            'transaction_value_usd': [300, 800],
            'transaction_type': ['withdrawal', 'withdrawal']
        })

        result = processor.calculate_tvl_metrics(deposits, withdrawals)

        assert 'tvl' in result.columns
        assert 'daily_net_flow' in result.columns
        assert 'tvl_growth' in result.columns
        assert 'ALL_POOLS' in result['pool'].values  # Protocol-wide metrics

    def test_calculate_tvl_metrics_empty_data(self, processor):
        """Test TVL calculation with empty data"""
        empty_df = pd.DataFrame(columns=['pool', 'timestamp_', 'transaction_value_usd', 'transaction_type'])

        result = processor.calculate_tvl_metrics(empty_df, empty_df)

        assert len(result) == 0
        assert 'tvl' in result.columns


class TestVolumeCalculations:
    """Test volume calculation functionality"""

    @patch('scripts.refactor_pools_data.flatten_json_column')
    @patch('scripts.refactor_pools_data.format_datetimes')
    def test_calculate_volume_metrics_with_nested_pool(self, mock_format_dates,
                                                     mock_flatten, processor,
                                                     sample_volume_data):
        """Test volume calculation with nested pool data"""
        # Mock the flattening of pool column
        flattened_data = sample_volume_data.copy()
        flattened_data['pool_name'] = ['USDC/USDT', 'BTC/USDC']
        flattened_data['pool_token0_symbol'] = ['USDC', 'BTC']
        flattened_data['pool_token1_symbol'] = ['USDT', 'USDC']
        del flattened_data['pool']  # Remove original nested column

        mock_flatten.return_value = flattened_data
        mock_format_dates.return_value = flattened_data

        with patch.object(processor, '_format_volume_amounts', return_value=flattened_data):
            result = processor.calculate_volume_metrics(sample_volume_data)

        assert len(result) > 0
        assert 'volume' in result.columns
        assert 'volume_7d_ma' in result.columns

    def test_calculate_volume_metrics_empty_data(self, processor):
        """Test volume calculation with empty data"""
        result = processor.calculate_volume_metrics(pd.DataFrame())

        assert len(result) == 0
        assert 'volume' in result.columns

    def test_calculate_volume_metrics_none_data(self, processor):
        """Test volume calculation with None data"""
        result = processor.calculate_volume_metrics(None)

        assert len(result) == 0
        assert 'volume' in result.columns


class TestFeeCalculations:
    """Test fee calculation functionality"""

    @patch('scripts.refactor_pools_data.flatten_json_column')
    @patch('scripts.refactor_pools_data.format_datetimes')
    def test_calculate_fee_metrics_with_volume(self, mock_format_dates, mock_flatten,
                                             processor, sample_fee_data):
        """Test fee calculation with volume data for fee rate"""
        # Mock flattened fee data
        flattened_fees = sample_fee_data.copy()
        flattened_fees['pool_name'] = ['USDC/USDT', 'BTC/USDC']
        flattened_fees['pool_token0_symbol'] = ['USDC', 'BTC']
        flattened_fees['pool_token1_symbol'] = ['USDT', 'USDC']
        del flattened_fees['pool']

        mock_flatten.return_value = flattened_fees
        mock_format_dates.return_value = flattened_fees

        # Mock volume data
        volume_data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01', '2022-01-02']),
            'pool': ['USDC/USDT', 'BTC/USDC'],
            'volume': [10000, 5000]
        })

        with patch.object(processor, '_format_fee_amounts', return_value=flattened_fees):
            result = processor.calculate_fee_metrics(sample_fee_data, volume_data)

        assert 'fee_rate' in result.columns
        assert 'cumulative_fees' in result.columns

    def test_calculate_fee_metrics_no_volume(self, processor, sample_fee_data):
        """Test fee calculation without volume data"""
        with patch('scripts.refactor_pools_data.flatten_json_column', return_value=sample_fee_data):
            with patch('scripts.refactor_pools_data.format_datetimes', return_value=sample_fee_data):
                with patch.object(processor, '_format_fee_amounts', return_value=sample_fee_data):
                    result = processor.calculate_fee_metrics(sample_fee_data, None)

        assert 'fee_rate' in result.columns
        assert result['fee_rate'].iloc[0] == 0  # Should default to 0 without volume


class TestUserMetrics:
    """Test user metrics calculation"""

    def test_calculate_user_metrics(self, processor):
        """Test user metrics calculation"""
        deposits = pd.DataFrame({
            'timestamp_': pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02']),
            'pool': ['Pool1', 'Pool1', 'Pool1'],
            'sender': ['user1', 'user2', 'user1'],
            'transaction_value_usd': [1000, 2000, 1500],
            'transaction_type': ['deposit', 'deposit', 'deposit']
        })

        withdrawals = pd.DataFrame({
            'timestamp_': pd.to_datetime(['2022-01-03']),
            'pool': ['Pool1'],
            'sender': ['user3'],
            'transaction_value_usd': [800],
            'transaction_type': ['withdrawal']
        })

        result = processor.calculate_user_metrics(deposits, withdrawals)

        assert 'unique_users' in result.columns
        assert 'avg_tx_size' in result.columns
        assert 'user_retention_rate' in result.columns
        assert len(result) > 0


class TestHealthMetrics:
    """Test pool health metrics calculation"""

    def test_calculate_health_metrics(self, processor):
        """Test health metrics calculation"""
        tvl_data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01', '2022-01-02']),
            'pool': ['Pool1', 'Pool1'],
            'tvl': [10000, 12000],
            'daily_net_flow': [1000, 2000]
        })

        volume_data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01', '2022-01-02']),
            'pool': ['Pool1', 'Pool1'],
            'volume': [5000, 6000]
        })

        fee_data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01', '2022-01-02']),
            'pool': ['Pool1', 'Pool1'],
            'daily_fees': [50, 60],
            'fee_rate': [0.01, 0.01]
        })

        result = processor.calculate_health_metrics(tvl_data, volume_data, fee_data)

        assert 'volume_tvl_ratio' in result.columns
        assert 'fee_apy' in result.columns
        assert 'efficiency_score' in result.columns
        assert not result['efficiency_score'].isna().any()


class TestSummaryStats:
    """Test summary statistics creation"""

    def test_create_summary_stats(self, processor):
        """Test summary statistics creation"""
        # Create mock metrics object
        health_data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02']),
            'pool': ['Pool1', 'ALL_POOLS', 'Pool1'],
            'tvl': [10000, 15000, 12000],
            'volume': [5000, 7000, 6000],
            'daily_fees': [50, 70, 60],
            'fee_apy': [0.1825, 0.1700, 0.1825],  # 18.25%, 17%, 18.25%
            'efficiency_score': [0.8, 0.75, 0.85]
        })

        tvl_data = pd.DataFrame({
            'pool': ['Pool1', 'Pool1'],
            'tvl': [10000, 12000]
        })

        volume_data = pd.DataFrame({
            'pool': ['Pool1', 'Pool1'],
            'volume': [5000, 6000]
        })

        metrics = PoolMetrics(
            tvl_metrics=tvl_data,
            volume_metrics=volume_data,
            fee_metrics=pd.DataFrame(),
            user_metrics=pd.DataFrame(),
            health_metrics=health_data,
            summary_stats={}
        )

        result = processor.create_summary_stats(metrics)

        assert 'protocol' in result
        assert 'top_pools' in result
        assert 'trends' in result
        assert 'snapshot_date' in result
        assert result['protocol']['total_tvl'] > 0


class TestDataValidation:
    """Test data validation and edge cases"""

    def test_handle_nan_values_in_calculations(self, processor):
        """Test handling of NaN values in calculations"""
        data_with_nans = pd.DataFrame({
            'pool': ['Pool1', 'Pool2'],
            'tvl': [10000, np.nan],
            'volume': [np.nan, 5000],
            'daily_fees': [50, 60]
        })

        # Test that calculations handle NaN appropriately
        result = processor.calculate_health_metrics(data_with_nans, data_with_nans, data_with_nans)

        # Should not contain infinite values
        assert not np.isinf(result.select_dtypes(include=[np.number])).any().any()
        # Should handle NaN appropriately (filled with 0 or reasonable defaults)
        assert not result['volume_tvl_ratio'].isna().any()

    def test_handle_infinite_values(self, processor):
        """Test handling of infinite values"""
        data = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01']),
            'pool': ['Pool1'],
            'tvl': [0],  # Will cause division by zero
            'volume': [1000],
            'daily_fees': [10]
        })

        result = processor.calculate_health_metrics(data, data, data)

        # Should replace inf values with 0
        assert not np.isinf(result.select_dtypes(include=[np.number])).any().any()

    def test_empty_dataframe_handling(self, processor):
        """Test handling of empty DataFrames"""
        empty_df = pd.DataFrame()

        # Should not raise exceptions
        tvl_result = processor.calculate_tvl_metrics(empty_df, empty_df)
        volume_result = processor.calculate_volume_metrics(empty_df)
        fee_result = processor.calculate_fee_metrics(empty_df, empty_df)

        assert len(tvl_result) == 0
        assert len(volume_result) == 0
        assert len(fee_result) == 0

    def test_data_type_consistency(self, processor, sample_deposit_data):
        """Test data type consistency after processing"""
        with patch.object(processor, '_clean_transaction_data') as mock_clean:
            cleaned_data = sample_deposit_data.copy()
            cleaned_data['timestamp_'] = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03'])
            cleaned_data['transaction_value_usd'] = [1000.0, 500.0, 750.0]
            cleaned_data['transaction_type'] = 'deposit'
            cleaned_data['pool'] = 'Pool1'

            mock_clean.return_value = cleaned_data

            result = processor.calculate_tvl_metrics(cleaned_data, pd.DataFrame())

            # Check data types
            assert result['date'].dtype == 'datetime64[ns]'
            assert pd.api.types.is_numeric_dtype(result['tvl'])
            assert pd.api.types.is_string_dtype(result['pool'])


class TestErrorHandling:
    """Test error handling scenarios"""

    def test_malformed_json_in_pool_column(self, processor):
        """Test handling of malformed JSON in pool column"""
        malformed_data = pd.DataFrame({
            'pool': ['invalid_json', {'valid': 'json'}],
            'timestamp': [1640995200, 1641081600],
            'totalVolume0': ['1000', '2000']
        })

        # Should handle malformed data gracefully
        with patch('scripts.refactor_pools_data.flatten_json_column') as mock_flatten:
            mock_flatten.side_effect = [malformed_data]  # Return as-is
            result = processor.calculate_volume_metrics(malformed_data)

            assert len(result) >= 0  # Should not crash

    def test_missing_required_columns(self, processor):
        """Test handling of missing required columns"""
        incomplete_data = pd.DataFrame({
            'some_column': [1, 2, 3]
        })

        # Should handle missing columns gracefully
        result = processor.calculate_volume_metrics(incomplete_data)
        assert len(result) == 0


class TestIntegration:
    """Integration tests for complete pipeline"""

    @patch('scripts.refactor_pools_data.SubgraphClient.get_subgraph_data')
    def test_full_pipeline_integration(self, mock_get_data, processor):
        """Test full data processing pipeline"""
        # Mock all data sources
        mock_get_data.side_effect = [
            [{'contractId_': '0x123', 'timestamp_': 1640995200, 'amount0': '1000', 'amount1': '2000', 'sender': 'user1'}],
            [{'contractId_': '0x123', 'timestamp_': 1641081600, 'amount0': '500', 'amount1': '1000', 'sender': 'user2'}],
            [],  # Empty volume data
            []   # Empty fee data
        ]

        # Mock the formatting functions
        with patch('scripts.refactor_pools_data.format_datetimes') as mock_dates, \
             patch('scripts.refactor_pools_data.format_pool_token_columns') as mock_tokens, \
             patch('scripts.refactor_pools_data.add_pool_usd_conversions') as mock_usd:

            # Configure mocks to return processed data
            def mock_date_format(df, cols):
                df = df.copy()
                df['timestamp_'] = pd.to_datetime(['2022-01-01', '2022-01-02'])
                return df

            def mock_token_format(df, contract_col, pool_pairs):
                df = df.copy()
                df['amount0'] = [1000.0, 500.0]
                df['amount1'] = [2000.0, 1000.0]
                return df

            def mock_usd_conversion(df, contract_col, pool_pairs, token_map):
                df = df.copy()
                df['amount0_usd'] = [1000.0, 500.0]
                df['amount1_usd'] = [2000.0, 1000.0]
                df['pool'] = ['Pool1', 'Pool1']
                return df

            mock_dates.side_effect = mock_date_format
            mock_tokens.side_effect = mock_token_format
            mock_usd.side_effect = mock_usd_conversion

            # Run full pipeline
            result = processor.process_all_data()

            # Verify result structure
            assert isinstance(result, PoolMetrics)
            assert result.tvl_metrics is not None
            assert result.volume_metrics is not None
            assert result.fee_metrics is not None
            assert result.user_metrics is not None
            assert result.health_metrics is not None
            assert result.summary_stats is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])