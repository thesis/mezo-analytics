#!/usr/bin/env python3
"""
Automated requirements.txt updater script.

This script:
1. Scans the codebase for all import statements
2. Checks currently installed package versions
3. Updates requirements.txt with current versions
4. Maintains manual comments and structure
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Set, List, Tuple
# pkg_resources is deprecated - using pip freeze instead for package versions


def get_project_root() -> Path:
    """Get the project root directory."""
    script_path = Path(__file__).parent
    return script_path.parent  # Go up one level from scripts/


def scan_imports(project_root: Path) -> Set[str]:
    """Scan Python files for import statements."""
    imports = set()
    
    # Directories to scan
    scan_dirs = ['scripts', 'mezo', 'notebooks']
    
    for dir_name in scan_dirs:
        dir_path = project_root / dir_name
        if not dir_path.exists():
            continue
            
        for py_file in dir_path.rglob('*.py'):
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Find import statements
                import_patterns = [
                    r'^import\s+([a-zA-Z0-9_][a-zA-Z0-9_.-]*)',
                    r'^from\s+([a-zA-Z0-9_][a-zA-Z0-9_.-]*)\s+import'
                ]
                
                for line in content.split('\n'):
                    line = line.strip()
                    for pattern in import_patterns:
                        match = re.match(pattern, line)
                        if match:
                            module_name = match.group(1)
                            # Skip relative imports and built-in modules
                            if not module_name.startswith('.') and not module_name.startswith('_'):
                                imports.add(module_name.split('.')[0])
                                
            except (UnicodeDecodeError, IOError):
                print(f"Warning: Could not read {py_file}")
                continue
    
    return imports


def get_installed_packages() -> Dict[str, str]:
    """Get currently installed packages and their versions."""
    installed = {}
    
    try:
        result = subprocess.run([sys.executable, '-m', 'pip', 'freeze'], 
                              capture_output=True, text=True, check=True)
        
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line and '==' in line and not line.startswith('-e'):
                name, version = line.split('==', 1)
                installed[name.lower().replace('_', '-')] = version
                
    except subprocess.CalledProcessError as e:
        print(f"Error getting installed packages: {e}")
        
    return installed


def get_package_mapping() -> Dict[str, str]:
    """Map import names to package names."""
    return {
        # Common mappings where import name != package name
        'cv2': 'opencv-python',
        'PIL': 'Pillow',
        'yaml': 'PyYAML',
        'dateutil': 'python-dateutil',
        'dotenv': 'python-dotenv',
        'google': 'google-cloud-bigquery',  # Main Google package we use
        'jwt': 'PyJWT',
        'requests_oauthlib': 'requests-oauthlib',
        'sklearn': 'scikit-learn',
        'serial': 'pyserial',
        # Keep original names for these
        'pandas': 'pandas',
        'numpy': 'numpy',
        'matplotlib': 'matplotlib',
        'web3': 'web3',
        'supabase': 'supabase',
        'requests': 'requests',
        'pyarrow': 'pyarrow',
        'jupyterlab': 'jupyterlab',
        'jupyter': 'jupyter',
        'ipython': 'ipython',
        'notebook': 'notebook',
        'pytest': 'pytest',
        'ruff': 'ruff',
        'mkdocs': 'mkdocs',
    }


def filter_stdlib_modules(imports: Set[str]) -> Set[str]:
    """Filter out standard library modules."""
    stdlib_modules = {
        'os', 'sys', 'json', 'time', 'datetime', 'collections', 'itertools',
        'functools', 'operator', 'pathlib', 'subprocess', 'threading', 
        'multiprocessing', 'queue', 'logging', 'unittest', 'argparse',
        'configparser', 'csv', 'sqlite3', 'urllib', 'http', 'email',
        'hashlib', 'hmac', 'secrets', 'uuid', 'random', 'math', 'statistics',
        'decimal', 'fractions', 'cmath', 'numbers', 'array', 'struct',
        'codecs', 'unicodedata', 'stringprep', 're', 'difflib', 'textwrap',
        'readline', 'rlcompleter', 'pickle', 'copyreg', 'shelve', 'marshal',
        'dbm', 'zlib', 'gzip', 'bz2', 'lzma', 'zipfile', 'tarfile',
        'typing', 'copy', 'pprint', 'reprlib', 'enum', 'graphlib',
        'weakref', 'types', 'abc', 'contextlib', 'traceback', 'gc',
        'inspect', 'site', 'warnings', 'dataclasses', 'asyncio'
    }
    
    return {imp for imp in imports if imp not in stdlib_modules}


def categorize_packages(packages: Dict[str, str]) -> Dict[str, List[Tuple[str, str]]]:
    """Categorize packages by their purpose."""
    categories = {
        'Core data processing': [],
        'Web3 and blockchain': [],
        'HTTP requests': [],
        'Environment management': [],
        'Database and API clients': [],
        'Data visualization': [],
        'Jupyter and development': [],
        'Development and testing': [],
        'Code quality': [],
        'Machine learning': [],
        'Documentation': [],
        'Development setup': []
    }
    
    category_mapping = {
        'pandas': 'Core data processing',
        'numpy': 'Core data processing',
        'web3': 'Web3 and blockchain',
        'requests': 'HTTP requests',
        'python-dotenv': 'Environment management',
        'google-cloud-bigquery': 'Database and API clients',
        'google-auth': 'Database and API clients',
        'google-api-core': 'Database and API clients',
        'supabase': 'Database and API clients',
        'pyarrow': 'Database and API clients',
        'db-dtypes': 'Database and API clients',
        'matplotlib': 'Data visualization',
        'ipython': 'Jupyter and development',
        'jupyterlab': 'Jupyter and development',
        'notebook': 'Jupyter and development',
        'jupyter': 'Jupyter and development',
        'pytest': 'Development and testing',
        'ruff': 'Code quality',
        'scikit-learn': 'Machine learning',
        'mkdocs': 'Documentation',
        'pip': 'Development setup'
    }
    
    for package, version in packages.items():
        category = category_mapping.get(package, 'Database and API clients')
        categories[category].append((package, version))
    
    # Sort packages within each category
    for category in categories:
        categories[category].sort(key=lambda x: x[0])
    
    return categories


def generate_requirements_content(categorized_packages: Dict[str, List[Tuple[str, str]]]) -> str:
    """Generate the requirements.txt content."""
    content = ["-e .", ""]
    
    for category, packages in categorized_packages.items():
        if not packages:
            continue
            
        content.append(f"# {category}")
        for package, version in packages:
            # Use >= for minimum version requirements
            content.append(f"{package}>={version}")
        content.append("")
    
    # Add built-in modules comment
    content.extend([
        "# Built-in modules (no install needed):",
        "# - hashlib, json, time, os, sys, subprocess",
        "# - decimal, datetime, functools, typing, pathlib",
        "# - traceback, collections, itertools, re"
    ])
    
    return '\n'.join(content)


def backup_requirements(requirements_path: Path) -> None:
    """Create a backup of the current requirements.txt."""
    if requirements_path.exists():
        backup_path = requirements_path.with_suffix('.txt.backup')
        backup_path.write_text(requirements_path.read_text())
        print(f"📋 Backed up current requirements.txt to {backup_path}")


def main():
    """Main function to update requirements.txt."""
    print("🔄 Updating requirements.txt...")
    
    project_root = get_project_root()
    requirements_path = project_root / 'requirements.txt'
    
    # Backup current requirements
    backup_requirements(requirements_path)
    
    # Scan for imports
    print("🔍 Scanning codebase for imports...")
    raw_imports = scan_imports(project_root)
    print(f"Found {len(raw_imports)} unique import statements")
    
    # Filter out stdlib modules
    filtered_imports = filter_stdlib_modules(raw_imports)
    print(f"Filtered to {len(filtered_imports)} external packages")
    
    # Get package mapping
    package_mapping = get_package_mapping()
    
    # Get installed packages
    print("📦 Getting installed package versions...")
    installed = get_installed_packages()
    
    # Map imports to actual packages and get versions
    required_packages = {}
    for imp in filtered_imports:
        package_name = package_mapping.get(imp, imp)
        
        # Try different name variations
        possible_names = [
            package_name,
            package_name.lower(),
            package_name.replace('_', '-'),
            package_name.replace('-', '_')
        ]
        
        for name in possible_names:
            if name in installed:
                required_packages[name] = installed[name]
                break
        else:
            print(f"⚠️ Warning: Could not find installed version for '{imp}' (mapped to '{package_name}')")
    
    print(f"📊 Found versions for {len(required_packages)} packages")
    
    # Categorize packages
    categorized = categorize_packages(required_packages)
    
    # Generate new requirements content
    new_content = generate_requirements_content(categorized)
    
    # Write new requirements.txt
    requirements_path.write_text(new_content)
    
    print(f"✅ Updated requirements.txt with {len(required_packages)} packages")
    print(f"📁 Requirements file: {requirements_path}")
    
    # Show what was updated
    for category, packages in categorized.items():
        if packages:
            print(f"  {category}: {len(packages)} packages")


if __name__ == "__main__":
    main()