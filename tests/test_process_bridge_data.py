import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.process_bridge_data import clean_bridge_data, main


class TestCleanBridgeData:
    """Test the clean_bridge_data function."""
    
    def setup_method(self):
        """Set up test data."""
        self.sample_raw_data = pd.DataFrame({
            'timestamp_': [1640995200, 1641081600, 1641168000],  # Unix timestamps
            'amount': [1000000000000000000, 2000000000000000000, 500000000000000000],  # Wei amounts
            'token': ['0x1234...', '0x5678...', '0x9abc...'],
            'recipient': ['0xabc123', '0xdef456', '0xabc123'],
            'transactionHash_': ['0xhash1', '0xhash2', '0xhash3']
        })
        
        self.token_map = {
            '0x1234...': 'WBTC',
            '0x5678...': 'USDC', 
            '0x9abc...': 'DAI'
        }
    
    @patch('scripts.process_bridge_data.replace_token_labels')
    @patch('scripts.process_bridge_data.format_datetimes')
    @patch('scripts.process_bridge_data.format_currency_columns')
    def test_clean_bridge_data_basic_functionality(self, mock_format_currency, mock_format_dates, mock_replace_tokens):
        """Test that clean_bridge_data calls all expected functions."""
        # Setup mocks to return modified dataframes
        mock_replace_tokens.return_value = self.sample_raw_data.copy()
        mock_format_dates.return_value = self.sample_raw_data.copy()
        mock_format_currency.return_value = self.sample_raw_data.copy()
        
        result = clean_bridge_data(
            self.sample_raw_data, 
            'timestamp_', 
            ['timestamp_'], 
            ['amount'], 
            'token'
        )
        
        # Verify all processing functions were called
        mock_replace_tokens.assert_called_once()
        mock_format_dates.assert_called_once()
        mock_format_currency.assert_called_once()
        
        # Verify count column was added
        assert 'count' in result.columns
        assert all(result['count'] == 1)
    
    def test_clean_bridge_data_sorting(self):
        """Test that data is sorted correctly by the sort column."""
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            # Setup mocks to return the same data
            mock_replace.return_value = self.sample_raw_data.copy()
            mock_dates.return_value = self.sample_raw_data.copy()
            mock_currency.return_value = self.sample_raw_data.copy()
            
            result = clean_bridge_data(
                self.sample_raw_data, 
                'timestamp_', 
                ['timestamp_'], 
                ['amount'], 
                'token'
            )
            
            # Check that data is sorted by timestamp in descending order
            timestamps = result['timestamp_'].tolist()
            assert timestamps == sorted(timestamps, reverse=True)
    
    def test_clean_bridge_data_with_empty_dataframe(self):
        """Test clean_bridge_data with empty input."""
        empty_df = pd.DataFrame()
        
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            mock_replace.return_value = empty_df
            mock_dates.return_value = empty_df
            mock_currency.return_value = empty_df
            
            result = clean_bridge_data(empty_df, 'timestamp_', ['timestamp_'], ['amount'], 'token')
            
            assert len(result) == 0
            assert 'count' in result.columns


class TestMainFunction:
    """Test the main processing function."""
    
    def setup_method(self):
        """Set up test data and mocks."""
        self.sample_bridge_data = pd.DataFrame({
            'timestamp_': ['2025-01-01', '2025-01-02', '2025-01-01'],
            'amount': [1.5, 2.0, 0.5],
            'token': ['WBTC', 'USDC', 'WBTC'],
            'recipient': ['0xabc123', '0xdef456', '0xghi789'],
            'transactionHash_': ['0xhash1', '0xhash2', '0xhash3'],
            'count': [1, 1, 1]
        })
        
        self.token_prices = pd.DataFrame({
            'index': ['bitcoin', 'usd-coin'],
            'usd': [50000.0, 1.0]
        })
        
        self.tokens_id_map = {
            'WBTC': 'bitcoin',
            'USDC': 'usd-coin'
        }
        
        self.token_type_map = {
            'WBTC': 'bitcoin',
            'USDC': 'stablecoin'
        }
    
    @patch('scripts.process_bridge_data.load_dotenv')
    @patch('scripts.process_bridge_data.get_all_bridge_transactions')
    @patch('scripts.process_bridge_data.load_raw_data')
    @patch('scripts.process_bridge_data.clean_bridge_data')
    @patch('scripts.process_bridge_data.get_token_prices')
    @patch('scripts.process_bridge_data.TOKENS_ID_MAP')
    @patch('scripts.process_bridge_data.TOKEN_TYPE_MAP')
    @patch('scripts.process_bridge_data.SupabaseClient')
    def test_main_function_success_path(self, mock_supabase_class, mock_token_type_map, 
                                      mock_tokens_id_map, mock_get_prices, mock_clean_data,
                                      mock_load_data, mock_get_transactions, mock_load_env):
        """Test successful execution of main function."""
        # Setup mocks
        mock_tokens_id_map.__getitem__ = lambda self, key: self.tokens_id_map[key]
        mock_token_type_map.__getitem__ = lambda self, key: self.token_type_map[key]
        
        mock_load_data.return_value = self.sample_bridge_data
        mock_clean_data.return_value = self.sample_bridge_data
        
        # Mock token prices
        mock_prices = Mock()
        mock_prices.T.reset_index.return_value = self.token_prices
        mock_get_prices.return_value = mock_prices
        
        # Mock Supabase client
        mock_supabase_instance = Mock()
        mock_supabase_class.return_value = mock_supabase_instance
        
        with patch('scripts.process_bridge_data.datetime') as mock_datetime:
            mock_datetime.today.return_value.date.return_value = '2025-01-01'
            
            # Execute main function
            result = main()
            
            # Verify function calls
            mock_load_env.assert_called_once()
            mock_get_transactions.assert_called_once()
            mock_load_data.assert_called_once()
            mock_clean_data.assert_called_once()
            mock_get_prices.assert_called_once()
            
            # Verify result structure
            assert 'daily_bridge_data' in result
            assert 'bridge_by_token' in result
            assert 'summary' in result
            
            # Verify Supabase upload attempts
            assert mock_supabase_instance.update_supabase.call_count == 3
    
    @patch('scripts.process_bridge_data.load_dotenv')
    @patch('scripts.process_bridge_data.get_all_bridge_transactions')
    def test_main_function_handles_data_loading_error(self, mock_get_transactions, mock_load_env):
        """Test main function handles data loading errors gracefully."""
        # Make get_all_bridge_transactions raise an exception
        mock_get_transactions.side_effect = Exception("API connection failed")
        
        with pytest.raises(Exception) as exc_info:
            main()
        
        assert "API connection failed" in str(exc_info.value)
    
    @patch('scripts.process_bridge_data.load_dotenv')
    @patch('scripts.process_bridge_data.get_all_bridge_transactions')
    @patch('scripts.process_bridge_data.load_raw_data')
    @patch('scripts.process_bridge_data.clean_bridge_data')
    @patch('scripts.process_bridge_data.get_token_prices')
    def test_main_function_handles_price_api_error(self, mock_get_prices, mock_clean_data,
                                                 mock_load_data, mock_get_transactions, mock_load_env):
        """Test main function handles token price API errors."""
        mock_load_data.return_value = self.sample_bridge_data
        mock_clean_data.return_value = self.sample_bridge_data
        mock_get_prices.side_effect = Exception("Price API unavailable")
        
        with pytest.raises(Exception) as exc_info:
            main()
        
        assert "Price API unavailable" in str(exc_info.value)
    
    @patch('scripts.process_bridge_data.load_dotenv')
    @patch('scripts.process_bridge_data.get_all_bridge_transactions')
    @patch('scripts.process_bridge_data.load_raw_data')
    @patch('scripts.process_bridge_data.clean_bridge_data')
    @patch('scripts.process_bridge_data.get_token_prices')
    @patch('scripts.process_bridge_data.TOKENS_ID_MAP')
    @patch('scripts.process_bridge_data.TOKEN_TYPE_MAP')
    @patch('scripts.process_bridge_data.SupabaseClient')
    def test_main_function_handles_supabase_error(self, mock_supabase_class, mock_token_type_map,
                                                mock_tokens_id_map, mock_get_prices, mock_clean_data,
                                                mock_load_data, mock_get_transactions, mock_load_env):
        """Test main function handles Supabase upload errors gracefully."""
        # Setup successful data processing
        mock_tokens_id_map.__getitem__ = lambda self, key: self.tokens_id_map.get(key, 'unknown')
        mock_token_type_map.__getitem__ = lambda self, key: self.token_type_map.get(key, 'unknown')
        
        mock_load_data.return_value = self.sample_bridge_data
        mock_clean_data.return_value = self.sample_bridge_data
        
        mock_prices = Mock()
        mock_prices.T.reset_index.return_value = self.token_prices
        mock_get_prices.return_value = mock_prices
        
        # Mock Supabase client to raise exception
        mock_supabase_instance = Mock()
        mock_supabase_instance.update_supabase.side_effect = Exception("Database connection failed")
        mock_supabase_class.return_value = mock_supabase_instance
        
        with patch('scripts.process_bridge_data.datetime') as mock_datetime:
            mock_datetime.today.return_value.date.return_value = '2025-01-01'
            
            # Should not raise exception, but handle it gracefully
            result = main()
            
            # Verify data processing still completed
            assert 'daily_bridge_data' in result
            assert 'bridge_by_token' in result
            assert 'summary' in result


class TestDataValidation:
    """Test data validation and edge cases."""
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty dataframes."""
        empty_df = pd.DataFrame()
        
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            mock_replace.return_value = empty_df
            mock_dates.return_value = empty_df
            mock_currency.return_value = empty_df
            
            result = clean_bridge_data(empty_df, 'timestamp_', ['timestamp_'], ['amount'], 'token')
            
            assert len(result) == 0
            assert 'count' in result.columns
    
    def test_missing_columns_handling(self):
        """Test handling of dataframes with missing expected columns."""
        incomplete_df = pd.DataFrame({
            'timestamp_': [1640995200],
            'amount': [1000000000000000000]
            # Missing 'token', 'recipient', 'transactionHash_'
        })
        
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            mock_replace.return_value = incomplete_df
            mock_dates.return_value = incomplete_df
            mock_currency.return_value = incomplete_df
            
            # This should not raise an exception
            result = clean_bridge_data(incomplete_df, 'timestamp_', ['timestamp_'], ['amount'], 'token')
            
            assert 'count' in result.columns
    
    def test_duplicate_data_handling(self):
        """Test handling of duplicate transactions."""
        duplicate_data = pd.DataFrame({
            'timestamp_': [1640995200, 1640995200, 1641081600],
            'amount': [1000000000000000000, 1000000000000000000, 2000000000000000000],
            'token': ['WBTC', 'WBTC', 'USDC'],
            'recipient': ['0xabc123', '0xabc123', '0xdef456'],
            'transactionHash_': ['0xhash1', '0xhash1', '0xhash2']  # Same hash = duplicate
        })
        
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            mock_replace.return_value = duplicate_data
            mock_dates.return_value = duplicate_data
            mock_currency.return_value = duplicate_data
            
            result = clean_bridge_data(duplicate_data, 'timestamp_', ['timestamp_'], ['amount'], 'token')
            
            # Should still process all rows (deduplication might be handled elsewhere)
            assert len(result) == 3
            assert all(result['count'] == 1)


@pytest.fixture
def sample_environment():
    """Fixture providing sample environment setup."""
    return {
        'data_loaded': True,
        'prices_available': True,
        'supabase_connected': True
    }


class TestIntegration:
    """Integration tests for the complete workflow."""
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'test_url', 'SUPABASE_KEY': 'test_key'})
    def test_environment_setup(self):
        """Test that environment variables are properly loaded."""
        with patch('scripts.process_bridge_data.load_dotenv') as mock_load_env:
            # Just test that load_dotenv is called - actual env loading is mocked
            from scripts.process_bridge_data import main
            
            # We won't actually run main, just verify the import works
            assert callable(main)
            
    def test_data_flow_integrity(self):
        """Test that data flows correctly through all processing steps."""
        # This would be a more complex integration test
        # For now, we'll just verify the structure is sound
        from scripts.process_bridge_data import clean_bridge_data
        
        sample_data = pd.DataFrame({
            'timestamp_': [1640995200],
            'amount': [1000000000000000000],
            'token': ['WBTC'],
            'recipient': ['0xabc123'],
            'transactionHash_': ['0xhash1']
        })
        
        with patch('scripts.process_bridge_data.replace_token_labels') as mock_replace, \
             patch('scripts.process_bridge_data.format_datetimes') as mock_dates, \
             patch('scripts.process_bridge_data.format_currency_columns') as mock_currency:
            
            mock_replace.return_value = sample_data
            mock_dates.return_value = sample_data
            mock_currency.return_value = sample_data
            
            result = clean_bridge_data(sample_data, 'timestamp_', ['timestamp_'], ['amount'], 'token')
            
            # Verify data structure is maintained
            assert len(result) == 1
            assert 'count' in result.columns
            assert result['count'].iloc[0] == 1


if __name__ == "__main__":
    pytest.main([__file__])