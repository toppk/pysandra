


Developers Guide
================

Setting up your environment
---------------------------


1. install prerequisites (python >= 3.6, git, optionally lz4, snappy)
2. go into your workspace directory (e.g. `cd ~/workspace` )
3. checkout the project: `git clone https://github.com/toppk/pysandra`
4. switch to project directory: `cd pysandra`
5. Setup your virtualenv (e.g. `python -m venv venv/` )
6. active virtualenv (e.g. `source venv/bin/activate`)
7. install development dependencies
8.   `pip install -r  test-requirements.txt`
9.   `pip install -e .`
10.  `pip install -e tools/cqlshell`
11. run testsuit: `pytest`
12. run testing utilit `python -mcqlshell.testasync dml`
13. run testing utilit with DEBUGGING `PYSANDRA_LOG_LEVEL=DEBUG python -mcqlshell.testasync dml`
14. run test automation: `nox`
