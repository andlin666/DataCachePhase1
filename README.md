# DataCachePhase1
Simple project for DataCache utility functionality. The goal is to ease the use of Azure Object Store. The project consists of 
a collection of classes that encapsualte connections and common operations. It is built on top of the Azure Python SDK's.

Overall functionality consists of:

	(1) Creating connections to an Azure Object Storage Account.
	
	(2) Building a an in-memory map of containers and objects in the store.
	
	(3)	Managing a cache of the objects from the store. This includes making local copies of the objects 
		which is the primary use case or workflow. In Phase1 the cache is a local folder.
		
	(4) CRUD operations on the objects in the store and objects generated through usage of the cache. 
		Note, CRUD includes sampling. The sampling can be done at the object level or for a particular
		object in the situations where this makes sense.
		
	(5) Easily loading data into memory (with the option of sampling), either from the store directly or 
		from the cache. Supported file types are: text-delimited (csv, tsv, etc.), images (jpg, png, etc.),
		and common Python serialized objects (DataFrames, Arrays, etc.).
		
	(6) Integrated logging and data tracking.

Phase1 does not include:

	(1) Creating an Account

	(2) Having a non-local cache

	(3) Pluggins for user defined types

