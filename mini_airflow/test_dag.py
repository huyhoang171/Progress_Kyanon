#!/usr/bin/env python3
"""
Test script to validate the place_metric_ingestion DAG
Runs locally without Docker to check for syntax and import errors
"""
import sys
import os
from pathlib import Path

# Add paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))
sys.path.insert(0, str(project_root / 'dags'))

def test_dag_integrity():
    """Test that the DAG loads without errors"""
    print("=" * 60)
    print("Testing DAG Integrity")
    print("=" * 60)
    
    try:
        # Import the DAG
        from dags.place_metric_ingestion import dag
        
        print(f"✓ DAG loaded successfully")
        print(f"  DAG ID: {dag.dag_id}")
        print(f"  Schedule: {dag.schedule_interval}")
        print(f"  Start date: {dag.start_date}")
        print(f"  Tags: {dag.tags}")
        
        # Check tasks
        tasks = dag.tasks
        print(f"\n✓ Found {len(tasks)} tasks:")
        for task in tasks:
            print(f"    - {task.task_id} ({task.__class__.__name__})")
        
        # Check dependencies
        print(f"\n✓ Task dependencies:")
        for task in tasks:
            if task.downstream_task_ids:
                for downstream in task.downstream_task_ids:
                    print(f"    {task.task_id} >> {downstream}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error loading DAG: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_extract_module():
    """Test the extract module"""
    print("\n" + "=" * 60)
    print("Testing Extract Module")
    print("=" * 60)
    
    try:
        from ingestion.mock_api_extract import fetch_places_from_api, extract_task
        
        print("✓ Extract module imports successful")
        print("  Functions: fetch_places_from_api, extract_task")
        
        # Test fetch function
        places = fetch_places_from_api()
        if places:
            print(f"✓ Mock API returned {len(places)} places")
            if len(places) > 0:
                sample = places[0]
                print(f"  Sample place keys: {list(sample.keys())[:5]}...")
                if 'totalScore' in sample:
                    print(f"  Sample rating: {sample['totalScore']}")
        else:
            print("⚠ Warning: No places returned (check data file)")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in extract module: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_transform_module():
    """Test the transform module"""
    print("\n" + "=" * 60)
    print("Testing Transform Module")
    print("=" * 60)
    
    try:
        from transform.calculate_average_rating import (
            calculate_average_rating_from_places, 
            transform_task
        )
        
        print("✓ Transform module imports successful")
        print("  Functions: calculate_average_rating_from_places, transform_task")
        
        # Test with sample data
        sample_places = [
            {'totalScore': 4.5},
            {'totalScore': 4.7},
            {'totalScore': 4.2},
            {'totalScore': None},  # Should be filtered out
        ]
        
        avg = calculate_average_rating_from_places(sample_places)
        expected_avg = (4.5 + 4.7 + 4.2) / 3
        
        if avg is not None:
            print(f"✓ Average calculation works")
            print(f"  Calculated: {avg}, Expected: {expected_avg:.2f}")
            assert abs(avg - expected_avg) < 0.01, "Average mismatch"
        else:
            print("✗ Average calculation returned None")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ Error in transform module: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_module():
    """Test the load module"""
    print("\n" + "=" * 60)
    print("Testing Load Module")
    print("=" * 60)
    
    try:
        from load.db_loader import (
            create_daily_metrics_table,
            insert_or_update_metric,
            load_task
        )
        
        print("✓ Load module imports successful")
        print("  Functions: create_daily_metrics_table, insert_or_update_metric, load_task")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in load module: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("PLACE METRIC INGESTION DAG - TEST SUITE")
    print("=" * 60 + "\n")
    
    results = {
        'DAG Integrity': test_dag_integrity(),
        'Extract Module': test_extract_module(),
        'Transform Module': test_transform_module(),
        'Load Module': test_load_module(),
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:.<40} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED - DAG is ready to deploy!")
    else:
        print("✗ SOME TESTS FAILED - Please fix errors before deploying")
    print("=" * 60 + "\n")
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
