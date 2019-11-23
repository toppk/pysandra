## NOTICE

This project has just started.  In order to get the SDLC complete, this project is published on github and pypi, but I don't recommend anyone use it just yet.

However, it is for anyone who wants

* to get involved in development,
* to give feedback,
* or are happy with it's very limited state

Please check back often, I hope to get this to a generally useful state this year.  Contributions, advice welcome, development is coodinated on github.

<p align="center">
  <a href="https://pysandra.readthedocs.org/"><img width="350" height="208" src="https://raw.githubusercontent.com/toppk/pysandra/master/docs/img/logo.png" alt='pysandra'></a>
</p>

<p align="center"><strong>pysandra</strong> <em>- An asyncio based Cassandra client for Python.</em></p>

<p align="center">
<a href="https://actions-badge.atrox.dev/toppk/pysandra/goto?ref=master"><img alt="Build Status" src="https://github.com/toppk/pysandra/workflows/Build%20Status/badge.svg" /></a>
<a href="https://codecov.io/gh/toppk/pysandra"><img src="https://codecov.io/gh/toppk/pysandra/branch/master/graph/badge.svg" alt="Coverage"></a>
<a href="https://pypi.org/project/pysandra/"><img src="https://badge.fury.io/py/pysandra.svg" alt="Package version"></a>
</p>

## Progress

In case you didn't read the notice, there's quite a limited set of functionality at the moment.

Things that exist:

* asyncio interface
* basic DML query (all results are just bytes at the moment)
* basic DDL query
* basic prepared statements
* ...
* see protocol.md for more details

Things that don't exist:
* any authnetication support
* any ssl support
* any connection support other then localhost :)
* sync support (version 2.0)
* full error handling
* results into native python types
* ...
* why are you still reading this.


## Goals

* cassandra client driver for asyncio based applications
* allow doing anything the wire protocol allows and servers tolerate

## Implementation

Currently targeting python 3.6 and above

Aims to be:

* correct
* fast
* efficient
* developer friendly

## Todos

* get real data
* paging
* Tests
* make user objects user friendly (exceptions, EventChage, Rows)
* AUTH
* batch
* client connection routing
* cqlshell


## License & Contributions

The pysandra project is dual-licensed under Apache 2.0 and MIT terms.
 
 See COPYRIGHT for full details

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
