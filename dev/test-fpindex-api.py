#!/usr/bin/env python3
"""
Test script for fpindex API
Demonstrates the current API endpoints that our proxy service needs to mimic
"""

import requests
import json
import msgpack
import sys

FPINDEX_URL = "http://localhost:6081"
INDEX_NAME = "test"

def test_health():
    """Test health endpoint"""
    print("Testing health endpoint...")
    try:
        resp = requests.get(f"{FPINDEX_URL}/_health")
        print(f"Health check: {resp.status_code} - {resp.text}")
        return resp.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_create_index():
    """Test index creation"""
    print(f"\nCreating index '{INDEX_NAME}'...")
    try:
        resp = requests.put(f"{FPINDEX_URL}/{INDEX_NAME}")
        print(f"Create index: {resp.status_code} - {resp.text}")
        return resp.status_code in [200, 409]  # 409 if already exists
    except Exception as e:
        print(f"Create index failed: {e}")
        return False

def test_single_fingerprint():
    """Test single fingerprint operations"""
    print("\nTesting single fingerprint operations...")
    
    # Insert fingerprint
    fp_id = 123456
    hashes = [100, 200, 300, 400, 500]
    
    try:
        resp = requests.put(
            f"{FPINDEX_URL}/{INDEX_NAME}/{fp_id}",
            json={"hashes": hashes}
        )
        print(f"Insert fingerprint {fp_id}: {resp.status_code} - {resp.text}")
        
        if resp.status_code != 200:
            return False
        
        # Check if fingerprint exists
        resp = requests.head(f"{FPINDEX_URL}/{INDEX_NAME}/{fp_id}")
        print(f"Check fingerprint exists: {resp.status_code}")
        
        # Get fingerprint info
        resp = requests.get(f"{FPINDEX_URL}/{INDEX_NAME}/{fp_id}")
        print(f"Get fingerprint info: {resp.status_code} - {resp.text}")
        
        return True
    except Exception as e:
        print(f"Single fingerprint test failed: {e}")
        return False

def test_bulk_update():
    """Test bulk update endpoint (main API we need to proxy)"""
    print("\nTesting bulk update...")
    
    changes = [
        {
            "insert": {
                "id": 789012,
                "hashes": [1000, 2000, 3000, 4000]
            }
        },
        {
            "insert": {
                "id": 345678,
                "hashes": [1500, 2500, 3500]
            }
        }
    ]
    
    try:
        # Test JSON format
        resp = requests.post(
            f"{FPINDEX_URL}/{INDEX_NAME}/_update",
            json={"changes": changes}
        )
        print(f"Bulk update (JSON): {resp.status_code} - {resp.text}")
        
        if resp.status_code != 200:
            return False
            
        # Test MessagePack format
        data = {"changes": changes}
        packed_data = msgpack.packb(data)
        
        resp = requests.post(
            f"{FPINDEX_URL}/{INDEX_NAME}/_update",
            data=packed_data,
            headers={"Content-Type": "application/vnd.msgpack"}
        )
        print(f"Bulk update (MessagePack): {resp.status_code} - {resp.text}")
        
        return resp.status_code == 200
    except Exception as e:
        print(f"Bulk update test failed: {e}")
        return False

def test_search():
    """Test search functionality"""
    print("\nTesting search...")
    
    query_hashes = [100, 200, 1000, 2000]  # Should match some of our inserted fingerprints
    
    try:
        resp = requests.post(
            f"{FPINDEX_URL}/{INDEX_NAME}/_search",
            json={
                "query": query_hashes,
                "limit": 10
            }
        )
        print(f"Search: {resp.status_code} - {resp.text}")
        
        if resp.status_code == 200:
            results = resp.json()
            print(f"Found {len(results.get('results', []))} matches")
            
        return resp.status_code == 200
    except Exception as e:
        print(f"Search test failed: {e}")
        return False

def test_delete():
    """Test fingerprint deletion"""
    print("\nTesting fingerprint deletion...")
    
    fp_id = 123456
    
    try:
        resp = requests.delete(f"{FPINDEX_URL}/{INDEX_NAME}/{fp_id}")
        print(f"Delete fingerprint {fp_id}: {resp.status_code} - {resp.text}")
        
        # Verify it's gone
        resp = requests.head(f"{FPINDEX_URL}/{INDEX_NAME}/{fp_id}")
        print(f"Check deleted fingerprint: {resp.status_code} (should be 404)")
        
        return True
    except Exception as e:
        print(f"Delete test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("fpindex API Test Suite")
    print("=====================")
    
    tests = [
        ("Health Check", test_health),
        ("Create Index", test_create_index),
        ("Single Fingerprint", test_single_fingerprint),
        ("Bulk Update", test_bulk_update),
        ("Search", test_search),
        ("Delete", test_delete),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            print(f"✅ {test_name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            results.append((test_name, False))
            print(f"❌ {test_name}: FAILED - {e}")
    
    print("\nTest Summary:")
    print("=============")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()