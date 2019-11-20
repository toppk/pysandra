## NOTICE

This project has just started.

In order to get the SDLC complete, this project is published on github and pypi.

This is only recommened for people who want

* to get involved in development,
* give feedback
* are happy with it's very limited state

Please check back often, I hope to get this to a generally useful state this year.

Contributions, advice welcome, development coodinated on github



<p align="center">
  <a href="https://pysandra.readthedocs.org/"><img width="350" height="208" src="https://raw.githubusercontent.com/toppk/pysandra/master/docs/img/logo.png" alt='pysandra'></a>
</p>

<p align="center"><strong>pysandra</strong> <em>- An asyncio based Cassandra client for Python.</em></p>

<p align="center">

<a href="https://actions-badge.atrox.dev/toppk/pysandra/goto?ref=master"><img alt="Build Status" src="https://github.com/toppk/pysandra/workflows/Build%20Status/badge.svg" /></a>

<a href="https://pypi.org/project/pysandra/"><img src="https://badge.fury.io/py/pysandra.svg" alt="Package version"></a>
</p>

## Progress

In case you didn't read the notice, there's quite a limited set of functionality at the moment.

Things that exist:

* asyncio interface
* basic DML query (all results are just bytes at the moment)
* basic prepared statements

Things that don't exist:
* any authnetication support
* any ssl support
* any connection support other then localhost :)
* sync support (version 2.0)
* DDL support
* full error handling
* results into native python types
* ...
* why are you still reading this.


## Goals

* cassandra client driver for asyncio based applications
* allow doing anything the wire protocol allows

## Implementation

Currently targeting python 3.6 and above

Aims to be:

* correct
* fast
* efficient
* developer friendly

## Todos

* Tests
* TLS
* AUTH

## Developing

$ cd pysandra
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r test-requirements.txt
$ pip install -e .


## License & Contributions

The pysandra project is dual-licensed under Apache 2.0 and MIT terms.
 
 See COPYRIGHT for full details

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
