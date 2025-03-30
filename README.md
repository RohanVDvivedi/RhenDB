# BeeDB
A relational database storage engine, akin to InnoDB and PostgreSQL's storage engine, built on top of TupleIndexer, TupleStore, VolatilePageStore and MinTxEngine.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [TupleIndexer](https://github.com/RohanVDvivedi/TupleIndexer)
 * [MinTxEngine](https://github.com/RohanVDvivedi/MinTxEngine)
 * [VolatilePageStore](https://github.com/RohanVDvivedi/VolatilePageStore)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/BeeDB.git`

**Build from source :**
 * `cd BeeDB`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lbeedb` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<beedb/beedb.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd BeeDB`
 * `sudo make uninstall`
