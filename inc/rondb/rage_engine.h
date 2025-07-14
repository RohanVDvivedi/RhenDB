#ifndef RAGE_ENGINE_H
#define RAGE_ENGINE_H

/*
	RageEngine is not a full blown storage engine (as the name could suggest) and hence the pun-intended name
	It provides a struct that encapsulates the pointers needed by the tupleindexer like
		* the storage engine context
		* the page access methods and
		* the page modification methods

	Populating this struct with the methods from MinTxEngine (for persistent ACID-compliant storage) OR the VolatilePageStore (for Volatile temporary-file backed storage)
	still wouldn't give you a storage engine, it is just a holder for the RonDB to allow us to easily call the tupleindexer methods and use it's exposed data structures
	So that you/we would't have to scavenge around the context, pam_p and pmm_p to pass them to the tupleindexer functions
	In a way it brings TupleIndexer, MinTxEngine and VolatilePageStore under a singlular umbrella, using the underlying format as dictated by the TupleStore

	Note: rage_engine is a flat small structure of pointers allowing you to pass it as parameters and return it by value, but try to pass it's pointer around instead
*/

#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

typedef struct rage_engine rage_engine;
struct rage_engine
{
	void* context;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
};

#endif