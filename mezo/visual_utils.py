"""Visual indicators and progress utilities for data processing scripts."""

import sys
import traceback
from typing import Dict, Any, Optional, Callable
from functools import wraps


class ProgressIndicators:
    """Visual indicators and progress symbols for terminal output."""
    
    SUCCESS = "✅"
    ERROR = "❌"
    WARNING = "⚠️"
    INFO = "ℹ️"
    LOADING = "🔄"
    ROCKET = "🚀"
    DATABASE = "🗄️"
    CHART = "📊"
    COIN = "💰"
    BRIDGE = "🌉"
    CLOCK = "⏰"
    CHECKMARK = "✓"
    CROSS = "✗"
    
    @staticmethod
    def print_header(title: str):
        """Print a formatted header with ASCII art."""
        border = "=" * (len(title) + 4)
        print(f"\n{border}")
        print(f"  {title}")
        print(f"{border}")
    
    @staticmethod
    def print_step(step: str, status: str = "start"):
        """Print a step with appropriate visual indicator."""
        if status == "start":
            print(f"\n{ProgressIndicators.LOADING} {step}...")
        elif status == "success":
            print(f"{ProgressIndicators.SUCCESS} {step}")
        elif status == "error":
            print(f"{ProgressIndicators.ERROR} {step}")
        elif status == "warning":
            print(f"{ProgressIndicators.WARNING} {step}")
        elif status == "info":
            print(f"{ProgressIndicators.INFO} {step}")
    
    @staticmethod
    def print_summary_box(title: str, items: Dict[str, Any]):
        """Print a summary box with data."""
        print(f"\n┌─ {title} ─")
        for key, value in items.items():
            if isinstance(value, float):
                print(f"│ {key}: ${value:,.2f}")
            elif isinstance(value, int):
                print(f"│ {key}: {value:,}")
            else:
                print(f"│ {key}: {value}")
        print("└" + "─" * (len(title) + 2))
    
    @staticmethod
    def print_ascii_bridge():
        """Print ASCII art of a bridge."""
        print("""
    🌉 DATA PROCESSING 🌉
    
         /|     /|
        / |____/ |
       /         |
      /___________| 
        """)
    
    @staticmethod
    def print_progress_bar(current: int, total: int, width: int = 50):
        """Print a progress bar."""
        if total == 0:
            return
        
        percent = current / total
        filled = int(width * percent)
        bar = "█" * filled + "░" * (width - filled)
        print(f"\rProgress: |{bar}| {percent:.1%} ({current}/{total})", end="", flush=True)


class ExceptionHandler:
    """Enhanced exception handling with visual feedback."""
    
    @staticmethod
    def handle_with_retry(func: Callable, max_retries: int = 3, delay: float = 1.0):
        """Execute function with retry logic and visual feedback."""
        import time
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    ProgressIndicators.print_step(f"Retry attempt {attempt + 1}/{max_retries}", "warning")
                    time.sleep(delay)
                
                return func()
                
            except Exception as e:
                if attempt == max_retries - 1:
                    ProgressIndicators.print_step(f"Failed after {max_retries} attempts: {str(e)}", "error")
                    raise
                else:
                    ProgressIndicators.print_step(f"Attempt {attempt + 1} failed: {str(e)}", "warning")
    
    @staticmethod
    def safe_execute(func: Callable, error_message: str = "Operation failed", 
                    return_on_error: Any = None) -> Any:
        """Safely execute a function with error handling and visual feedback."""
        try:
            return func()
        except Exception as e:
            ProgressIndicators.print_step(f"{error_message}: {str(e)}", "error")
            print(f"  {ProgressIndicators.INFO} Error details: {traceback.format_exc()}")
            return return_on_error
    
    @staticmethod
    def validate_dataframe(df, name: str, required_columns: Optional[list] = None) -> bool:
        """Validate DataFrame with visual feedback."""
        try:
            if df is None:
                ProgressIndicators.print_step(f"{name} is None", "error")
                return False
            
            if len(df) == 0:
                ProgressIndicators.print_step(f"{name} is empty", "warning")
                return False
            
            if required_columns:
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    ProgressIndicators.print_step(
                        f"{name} missing required columns: {missing_cols}", "error"
                    )
                    return False
            
            ProgressIndicators.print_step(f"{name} validation passed ({len(df)} rows)", "success")
            return True
            
        except Exception as e:
            ProgressIndicators.print_step(f"Error validating {name}: {str(e)}", "error")
            return False


def with_progress(description: str):
    """Decorator to add progress indicators to functions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ProgressIndicators.print_step(description, "start")
            try:
                result = func(*args, **kwargs)
                ProgressIndicators.print_step(f"{description} successful", "success")
                return result
            except Exception as e:
                ProgressIndicators.print_step(f"{description} - {str(e)}", "error")
                raise
        return wrapper
    return decorator


def safe_operation(error_message: str = "Operation failed", return_on_error: Any = None):
    """Decorator for safe operations with error handling."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return ExceptionHandler.safe_execute(
                lambda: func(*args, **kwargs), 
                error_message, 
                return_on_error
            )
        return wrapper
    return decorator