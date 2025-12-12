
import sys
import os
print("SYS.PATH:", sys.path)
try:
    import airflow
    print("AIRFLOW FILE:", airflow.__file__)
except ImportError as e:
    print("IMPORT ERROR:", e)
