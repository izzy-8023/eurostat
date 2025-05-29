#!/usr/bin/env python3
"""
Test script for Enhanced Eurostat DAG

This script verifies that the enhanced DAG can be loaded and all tasks are properly defined.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_enhanced_dag():
    """Test that the enhanced DAG loads correctly and has all expected tasks."""
    
    print("🧪 Testing Enhanced Eurostat DAG...")
    print("=" * 50)
    
    try:
        # Import the DAG
        from enhanced_eurostat_processor_dag import dag
        
        print(f"✅ DAG loaded successfully: {dag.dag_id}")
        print(f"📋 Description: {dag.description}")
        print(f"🏷️  Tags: {dag.tags}")
        
        # Check tasks
        tasks = list(dag.task_dict.keys())
        expected_tasks = [
            'validate_environment',
            'plan_processing', 
            'enhanced_download_datasets',
            'enhanced_load_to_database',
            'generate_dbt_models',
            'generate_topic_marts',
            'run_dbt_pipeline',
            'generate_pipeline_summary'
        ]
        
        print(f"\n📊 Tasks found: {len(tasks)}")
        for task in tasks:
            print(f"  ✅ {task}")
        
        # Verify all expected tasks are present
        missing_tasks = set(expected_tasks) - set(tasks)
        if missing_tasks:
            print(f"\n❌ Missing tasks: {missing_tasks}")
            return False
        
        extra_tasks = set(tasks) - set(expected_tasks)
        if extra_tasks:
            print(f"\n⚠️  Extra tasks: {extra_tasks}")
        
        print(f"\n🎯 Task Dependencies:")
        for task_id, task in dag.task_dict.items():
            upstream = [t.task_id for t in task.upstream_list]
            downstream = [t.task_id for t in task.downstream_list]
            if upstream or downstream:
                print(f"  {task_id}:")
                if upstream:
                    print(f"    ⬅️  Upstream: {upstream}")
                if downstream:
                    print(f"    ➡️  Downstream: {downstream}")
        
        print(f"\n✅ Enhanced DAG test completed successfully!")
        print(f"🚀 The DAG is ready for use!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing DAG: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_enhanced_dag()
    sys.exit(0 if success else 1) 