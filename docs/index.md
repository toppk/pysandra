!!! warning
     This project has recently started.  I don't recommend anyone use it just yet.

     However, it is for anyone who wants to get involved in development, to give feedback,
     or are happy with it's very limited state.

     Please check back often, I hope to get this to a generally useful state this year.  Contributions,
     advice welcome, development is coodinated on github.

<p align="center">
  <a href="https://pysandra.readthedocs.org/"><img width="350" height="208" src="https://raw.githubusercontent.com/toppk/pysandra/master/docs/img/logo.png" alt='pysandra'></a>
</p>

<p align="center"><strong>pysandra</strong> <em>- An asyncio based Cassandra client for Python.</em></p>

<p align="center">
<a href="https://actions-badge.atrox.dev/toppk/pysandra/goto?ref=master"><img alt="Build Status" src="https://github.com/toppk/pysandra/workflows/Build%20Status/badge.svg" /></a>
<a href="https://codecov.io/gh/toppk/pysandra"><img src="https://codecov.io/gh/toppk/pysandra/branch/master/graph/badge.svg" alt="Coverage"></a>
<a href="https://pypi.org/project/pysandra/"><img src="https://badge.fury.io/py/pysandra.svg" alt="Package version"></a>
</p>

The pysandra package can be used to create high-performance services that connect to cassandra using asyncio
based concurency.


Let's get started...

*This example works in python 3.7 or above (which introduced the asyncio.run() high level function,
but still requires creating functions.  To use async calls directly use `ipython`, or use Python 3.8
with `python -m asyncio`.*


```python
>>> import asyncio
>>> import pysandra
>>> async def test():
...    client = pysandra.Client(('localhost',9142), use_tls=True)
...    result = await client.execute("SELECT release_version FROM system.local")
...    print(list(result)[0][0]) # first row, first column
...
>>> asyncio.run(test())
```

## Progress

In case you didn't read the notice, there's quite a limited set of functionality at the moment.

Things that exist:

* asyncio interface
* basic query support (with results into native python types)
* basic DML support
* basic DDL query
* basic prepared statements
* basic TLS support
* ...
* see protocol.md for more details

Things that don't exist:

* any authentication support
* sync support (version 2.0)
* paging, batch support
* stable api
* friendly api
* connection to more then one cluster member
* ...
* why are you still reading this.


## Goals

* cassandra client driver for asyncio based applications
* allow doing anything the wire protocol allows and servers tolerate


## License & Contributions

The pysandra project is dual-licensed under Apache 2.0 and MIT terms.
 
 See COPYRIGHT for full details

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
