# Single Server Mode

Inoder supports running a single server for testing or prototyping  purposes, while the cluster mode works for production environments.

The single server mode has no replication/distribution so does not provide reliability, high availability or scalability. 

However, it implements the same functional API as the cluster mode.

## core data structures

* Catalog -> Space -> Shard -> Item

* MetaStore, storage of the space metadata

* ShardStore, storage of items within a shard





