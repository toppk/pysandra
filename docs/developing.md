pip install -e tools/cqlshell
PYSANDRA_LOG_LEVEL=DEBUG  python -mcqlshell.testasync foo
