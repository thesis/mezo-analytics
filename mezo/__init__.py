# import sys
# import os

# project_root = os.path.abspath(os.path.join(os.path.dirname("__file__"), "./"))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'), override=True)

python_path = os.getenv("PYTHONPATH")
if python_path and python_path not in os.sys.path:
    os.sys.path.insert(0, python_path)