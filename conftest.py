"""
Pytest configuration and fixtures.
"""

import sys
import os

# Add the project root to the path so we can use absolute imports
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
