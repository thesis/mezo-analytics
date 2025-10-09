import os
import pandas as pd

################################################
# testing functions
################################################

class tests:

    def quick_test(self, main, sample_size=1000):
        """
        Quick test function for development.
        Uses local CSV, samples 1000 rows, skips BigQuery.

        Args:
            main: The main function from the processing script
            sample_size: Number of rows to sample (default: 1000)

        Usage:
            from scripts.process_swaps_data import main
            from mezo.test_utils import tests

            results = tests().quick_test(main)
            results['pool_metrics']
        """
        return main(test_mode=True, sample_size=sample_size, skip_bigquery=True)

    def inspect_data(self, results, show_head=5):
        """
        Helper function to inspect all output dataframes.
        
        Usage:
            results = quick_test()
            inspect_data(results)
        """
        print(f"\n{'═' * 80}")
        print(f"{'DATA INSPECTION':^80}")
        print(f"{'═' * 80}\n")
        
        for name, df in results.items():
            if isinstance(df, pd.DataFrame):
                print(f"\n{name.upper()}")
                print(f"{'─' * 80}")
                print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
                print(f"\nColumns: {', '.join(df.columns.tolist())}")
                print(f"\nFirst {show_head} rows:")
                print(df.head(show_head).to_string())
                print(f"\nData types:")
                print(df.dtypes)
                print(f"\nMemory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                print(f"\n{'─' * 80}\n")

    def save_test_outputs(self, results, output_dir='./test_outputs'):
        """
        Save all test outputs to CSV files for manual inspection.
        
        Usage:
            results = quick_test()
            save_test_outputs(results)
        """
        
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in results.items():
            if isinstance(df, pd.DataFrame):
                filepath = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(filepath, index=False)
                print(f"✓ Saved {name} to {filepath}")
        
        print(f"\n✅ All outputs saved to {output_dir}/")
