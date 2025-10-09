import json
import pandas as pd
import os
from datetime import datetime, date
import numpy as np

def save_metrics_snapshot(metrics_data: dict, script_name: str = 'swaps'):
    """
    Save processing metrics to a JSON file for later report generation.
    
    Args:
        metrics_data: dict containing all metrics from the processing
        script_name: name of the script (used in filename)
    """
    metrics_dir = 'metrics_snapshots'
    os.makedirs(metrics_dir, exist_ok=True)
    
    serializable_metrics = {}
    
    for key, value in metrics_data.items():
        if isinstance(value, pd.DataFrame):
            # convert DataFrame to dict and handle special types
            df_dict = value.to_dict('records')
            for record in df_dict:
                for k, v in record.items():
                    if isinstance(v, (datetime, date)):
                        record[k] = v.isoformat()
                    elif isinstance(v, (np.integer, np.floating)):
                        record[k] = float(v)
                    elif isinstance(v, np.ndarray):
                        record[k] = v.tolist()
                    elif pd.isna(v):
                        record[k] = None
            serializable_metrics[key] = df_dict
        elif isinstance(value, (np.integer, np.floating)):
            serializable_metrics[key] = float(value)
        elif isinstance(value, np.ndarray):
            serializable_metrics[key] = value.tolist()
        else:
            serializable_metrics[key] = value
    
    metadata = {
        'generated_at': datetime.now().isoformat(),
        'script': script_name,
        'version': '1.0.0'
    }
    
    output = {
        'metadata': metadata,
        'metrics': serializable_metrics
    }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{metrics_dir}/{script_name}_metrics_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    latest_filename = f"{metrics_dir}/{script_name}_metrics_latest.json"
    with open(latest_filename, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\n📁 Metrics saved to:")
    print(f"   - {filename}")
    print(f"   - {latest_filename}")
    
    return filename
