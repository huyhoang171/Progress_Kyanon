#!/usr/bin/env python3
"""
Simple syntax check for the DAG file
Validates Python syntax without requiring Airflow to be installed
"""
import sys
import py_compile
from pathlib import Path

def check_syntax(file_path):
    """Check Python file syntax"""
    try:
        py_compile.compile(file_path, doraise=True)
        print(f"✓ {file_path.name} - Syntax OK")
        return True
    except py_compile.PyCompileError as e:
        print(f"✗ {file_path.name} - Syntax Error:")
        print(f"  {e}")
        return False

def main():
    project_root = Path(__file__).parent
    
    files_to_check = [
        project_root / 'dags' / 'place_metric_ingestion.py',
        project_root / 'src' / 'ingestion' / 'mock_api_extract.py',
        project_root / 'src' / 'transform' / 'calculate_average_rating.py',
        project_root / 'src' / 'load' / 'db_loader.py',
    ]
    
    print("=" * 60)
    print("Python Syntax Validation")
    print("=" * 60)
    
    results = []
    for file_path in files_to_check:
        if file_path.exists():
            results.append(check_syntax(file_path))
        else:
            print(f"✗ {file_path.name} - File not found")
            results.append(False)
    
    print("=" * 60)
    if all(results):
        print("✓ All files have valid Python syntax")
        return 0
    else:
        print("✗ Some files have syntax errors")
        return 1

if __name__ == '__main__':
    sys.exit(main())
