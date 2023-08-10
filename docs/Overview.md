# InodeDB: the CubeFS metadata subsystem redefined

## Why re-design the metadata subsystem?

1, increase the capx of a single metadata partition, from totally in-memory to RAM + disk

2, flexible support for file/object attributes & tags, i.e., schemaless

3, native file/object search - by name/tag/embedding etc.

We define InodeDB as a specialized distributed document database used as the metadata subsystem of cloud data lake and file or object storage, rather than a general-purpose key-value or relational database.

## Data Model

* Spaces, Shards, Items, Links

* Item, ino --> fields, ino is uint64, and items are partitioned into 'shards' by ino ranges. An item can be represented as a JSON document.

* Shard, the physical 'container' of items, working as the replication and storage unit. For simplicity we define each shard has a fixed ino range - 2^32.

* Space, the logical namespace of items. While InodeDB is schemaless by design, all items of a collection typically have a similar structure (although they can have different fields) and work for the same application.

* Link, <parent ino, name> --> <child ino,...>

* CubeFS' volume/partition/inode/dentry => InodeDB's space/shard/item/link

* Scalar Index, the index of scalar fields

* Embedding Index, the index of embeddings


## Architecture

InodeDB supports the single server mode and the distributed cluster mode.


An InodeDB cluster has several server roles:

* ShardServer, serving the shards
* Master, in charge of the cluster-level metadata
* Router, routing requests & merging reponses
* Agent, conversational agent

### Replication

multi-raft

### Storage

* metastore

* shardstore


### CDC

the Change Data Capture


## Building Blocks

* gRPC
* Rocksdb
* Prometheus

