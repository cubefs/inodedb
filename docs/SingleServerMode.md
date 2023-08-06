# Single Server Mode

InodeDB supports running a single server for testing or prototyping purposes, i.e., start it on a local machine. And the cluster mode works for production environments.

The single server mode has neither replication nor data distribution so does not guarantee reliability, high availability, and scalability. 

However, it implements the same functional API as the cluster mode.

## core data structures

* Catalog -> Space -> Shard -> Item

* MetaStore, storage of the space metadata

* ShardStore, storage of items within a shard





