import os
import sys
from unittest.mock import MagicMock

_root = os.path.dirname(os.path.dirname(__file__))

# functions/processor must come before scraper — both have a main.py,
# and test_processor.py patches "main.*" expecting the processor's module.
sys.path.insert(0, os.path.join(_root, "scraper"))
sys.path.insert(0, os.path.join(_root, "functions", "processor"))

# functions_framework 3.8.1 uses `from re import T` which Python 3.13 removed.
# Mock the whole package for tests — the @functions_framework.http decorator
# is just a no-op marker that has no effect on unit test execution.
if "functions_framework" not in sys.modules:
    _ff = MagicMock()
    _ff.http = lambda f: f  # make the decorator a transparent pass-through
    sys.modules["functions_framework"] = _ff
