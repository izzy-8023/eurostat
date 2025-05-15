import sys
import os

# Add the project root directory to sys.path so that 'dags' and 'scripts' can be imported
# This assumes conftest.py is in 'tests/' and the project root is its parent.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'scripts') # Also add scripts if needed for other tests

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    print(f"Added {PROJECT_ROOT} to sys.path for testing (for 'dags' package).")

# If you also need to directly import modules from the scripts folder in some tests:
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)
    print(f"Added {SCRIPTS_DIR} to sys.path for testing (for direct 'scripts' imports).") 