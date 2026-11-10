import sys
import os
import inspect
import asyncio
import unittest
import importlib.util

sys.path.insert(0, os.path.abspath("."))

def run_test_file(filepath):
    spec = importlib.util.spec_from_file_location("test_module", filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    passed = 0
    failed = 0
    print(f"\n[*] Running {os.path.basename(filepath)}:")
    
    # 1. Check for TestCase subclasses
    has_test_cases = False
    for name, obj in inspect.getmembers(mod):
        if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
            has_test_cases = True
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(obj)
            for test in suite:
                test_name = test._testMethodName
                result = unittest.TestResult()
                test.run(result)
                if result.wasSuccessful():
                    print(f"  ✓ {test_name}")
                    passed += 1
                else:
                    err = result.errors or result.failures
                    print(f"  ✗ {test_name}: {err}")
                    failed += 1

    # 2. Check for standalone test_* functions
    if not has_test_cases:
        for name, obj in inspect.getmembers(mod):
            if name.startswith("test_") and callable(obj):
                try:
                    if inspect.iscoroutinefunction(obj):
                        asyncio.run(obj())
                    else:
                        obj()
                    print(f"  ✓ {name}")
                    passed += 1
                except Exception as e:
                    print(f"  ✗ {name}: {e}")
                    failed += 1

    return passed, failed

if __name__ == "__main__":
    t1_p, t1_f = run_test_file("tests/test_concurrency.py")
    t2_p, t2_f = run_test_file("tests/test_mcp_server.py")
    t3_p, t3_f = run_test_file("tests/test_ruflo_adapter.py")
    total_p = t1_p + t2_p + t3_p
    total_f = t1_f + t2_f + t3_f
    print(f"\n==========================================")
    print(f"RESULTS: {total_p} passed, {total_f} failed.")
    print(f"==========================================")
    sys.exit(0 if total_f == 0 else 1)
