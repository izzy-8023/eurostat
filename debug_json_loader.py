#!/usr/bin/env python3
"""
Debug script to diagnose JSON loader issues
"""

import json
import ijson
import sys
import os

def debug_json_structure(json_path):
    """Debug function to inspect JSON structure and metadata"""
    print(f"🔍 Debugging JSON file: {json_path}")
    print("=" * 50)
    
    # Check file size
    file_size = os.path.getsize(json_path)
    print(f"File size: {file_size / (1024 * 1024):.2f} MB")
    
    # Test dimension IDs
    print("\n📊 Testing dimension IDs loading...")
    try:
        with open(json_path, 'rb') as f_id:
            dimension_ids = list(ijson.items(f_id, 'id.item'))
        print(f"✅ Found {len(dimension_ids)} dimension IDs: {dimension_ids}")
    except Exception as e:
        print(f"❌ Error loading dimension IDs: {e}")
        dimension_ids = None
    
    # Test dimension sizes
    print("\n📏 Testing dimension sizes loading...")
    try:
        with open(json_path, 'rb') as f_size:
            dimension_sizes = list(ijson.items(f_size, 'size.item'))
        print(f"✅ Found {len(dimension_sizes)} dimension sizes: {dimension_sizes}")
    except Exception as e:
        print(f"❌ Error loading dimension sizes: {e}")
        dimension_sizes = None
    
    # Test dimension details
    print("\n🏗️ Testing dimension details loading...")
    try:
        with open(json_path, 'rb') as f_dim_obj:
            all_dimension_data = next(ijson.items(f_dim_obj, 'dimension'), None)
        
        if all_dimension_data:
            print(f"✅ Found dimension data with {len(all_dimension_data)} dimensions")
            for dim_id, details in all_dimension_data.items():
                category_data = details.get('category', {})
                if isinstance(category_data, dict):
                    index_count = len(category_data.get('index', {}))
                    label_count = len(category_data.get('label', {}))
                    print(f"  - {dim_id}: {index_count} indices, {label_count} labels")
                else:
                    print(f"  - {dim_id}: category data is not a dict")
        else:
            print("❌ No dimension data found")
    except Exception as e:
        print(f"❌ Error loading dimension details: {e}")
        all_dimension_data = None
    
    # Test value data
    print("\n💰 Testing value data loading...")
    try:
        value_count = 0
        sample_values = []
        with open(json_path, 'rb') as f_values:
            for linear_index_str, value_content in ijson.kvitems(f_values, 'value'):
                value_count += 1
                if len(sample_values) < 5:  # Collect first 5 samples
                    sample_values.append((linear_index_str, value_content))
                if value_count >= 10:  # Stop after counting 10 to avoid long execution
                    break
        
        print(f"✅ Found at least {value_count} value entries")
        print("📋 Sample values:")
        for idx, val in sample_values:
            print(f"  - Index {idx}: {val}")
            
    except Exception as e:
        print(f"❌ Error loading value data: {e}")
        value_count = 0
    
    # Summary
    print("\n" + "=" * 50)
    print("🏁 SUMMARY:")
    print(f"  - File size: {file_size / (1024 * 1024):.2f} MB")
    print(f"  - Dimension IDs: {len(dimension_ids) if dimension_ids else 0}")
    print(f"  - Dimension sizes: {len(dimension_sizes) if dimension_sizes else 0}")
    print(f"  - Dimension details: {'✅' if all_dimension_data else '❌'}")
    print(f"  - Value entries: {value_count}+")
    
    # Check validation conditions
    validation_passed = (
        dimension_ids and 
        dimension_sizes and 
        all_dimension_data and 
        len(dimension_ids) == len(dimension_sizes)
    )
    print(f"  - Metadata validation: {'✅ PASS' if validation_passed else '❌ FAIL'}")
    
    if not validation_passed:
        print("\n🚨 DIAGNOSIS:")
        if not dimension_ids:
            print("  - dimension_ids is empty or None")
        if not dimension_sizes:
            print("  - dimension_sizes is empty or None")
        if not all_dimension_data:
            print("  - dimension_details is empty or None")
        if dimension_ids and dimension_sizes and len(dimension_ids) != len(dimension_sizes):
            print(f"  - Length mismatch: {len(dimension_ids)} IDs vs {len(dimension_sizes)} sizes")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python debug_json_loader.py <json_file_path>")
        sys.exit(1)
    
    json_path = sys.argv[1]
    debug_json_structure(json_path) 