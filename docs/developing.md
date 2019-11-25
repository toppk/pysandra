


Developers Guide
================

Setting up your environment
---------------------------


* install prerequisites (python >= 3.6, git, optionally lz4, snappy)
* Check out the project
```
cd ~/workspace
git clone https://github.com/toppk/pysandra
cd pysandra
```
* Setup your virtualenv:
```
python -m venv venv/
source venv/bin/activate
pip install -r  test-requirements.txt
pip install -e .
pip install -e tools/cqlshell
```

Since you were "inside" the virtualenv, you will have to run `source venv/bin/activate` each time you start a new shell to get end.  You can use `deactivate` a shell function that active installs, to leave the virtualenv.  Within the virtualenv, you can run pytest and the tools as follow:

```
pytest # run all tests
pytest -m "not live" # run only unit tests
python -mcqlshell.testasync dml  # run the dml test function
PYSANDRA_LOG_LEVEL=DEBUG python -mcqlshell.testasync dml  # run the dml test function with debug logging
testasync dml -d # same as above
```

There are scripts inside pysandra/scripts to help with these steps.  (e.g. scripts/bootstrap will setup a virtualenv and install the packages).  You can also run the test automation tool nox.  Nox will setup its own virtualenvs for execution, however you will still need to be inside of the virtualenv if you don't have nox installed elsewhere.  By default nox will do a complete pipeline of linting, formating, then test suits, and ensuring that all packages are up to date.  It will also run the test suit against several different python interpretors.  There are many ways you can run it, for example:

```no-highlight
nox # default run
nox -s check # just the linters
nox -s test # just the tests
nox -s test-3.6 # just the test on python36
nox -s test-3.6 -- -m "live"  # after the "--" the parameters are passed to pytest, in this case just run the tests that hit a cassandra instance on localhost:9042
nox -s serve # start the mkdocs server
```

You can think of nox as a replacement for a makefile.  There are some scripts that have similar execution inside of scripts/ as well

14. run test automation: `nox`


Touring the code base
-----------------------

```no-highlight
pysandra/ - most of the content in the top level are related to SDLC
   pysandra/ - the driver code
         /__about__.py
         /client.py - this is the entrypoint for users, creates a connection for sending requests
         /connection.py - connect holds connection details, creates a dispatcher to communicate
         /constants.py - constants for the wireprotocol as well as driver settings (timeouts, defaults, etc)
         /core.py - some generic classes in use by other modules
         /dispatcher.py - handles the communications between the client and protocol
         /exceptions.py - exceptions for the driver
         /__init__.py - the API
         /protocol.py - the encodes, decoders for the wire protocol
         /types.py - the data types that the API will send to the user
         /utils.py - some utility functions (e.g. loggin)
         /v4protocol.py - this speaks to the dispatcher to decide how request should look on the wire, and how to handle responses
   tests/ - pytest tests (there are subdirectories for live testing
   tools/ - currently contains testasync.py which has some driver example cases
   scripts/ - some SDLC related scripts (nox can do everything itself, execpt to provide the nox tool :)
```
