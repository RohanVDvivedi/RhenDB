# RonDB
A relational database storage engine, akin to InnoDB and PostgreSQL's storage engine, built on top of TupleIndexer, TupleStore, VolatilePageStore and MinTxEngine.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [TupleIndexer](https://github.com/RohanVDvivedi/TupleIndexer)
 * [TupleLargeTypes](https://github.com/RohanVDvivedi/TupleLargeTypes)
 * [MinTxEngine](https://github.com/RohanVDvivedi/MinTxEngine)
 * [VolatilePageStore](https://github.com/RohanVDvivedi/VolatilePageStore)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/RonDB.git`

**Build from source :**
 * `cd RonDB`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lrondb -ltuplestore -lcutlery` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<rondb/rondb.h>`
   * `#include<rondb/transaction_table.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd RonDB`
 * `sudo make uninstall`
