/*
 *
 * Copyright 2023 CubeFS authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*

# InodeDB: the CubeFS metadata subsystem redefined

## Why re-design the metadata subsystem?

1, increase the capx of a single metadata partition, from totally in-memory to RAM + disk

2, flexible support for file/object attributes & tags, i.e., schemaless

3, native file/object search - by name/tag/embedding etc.

## Concepts

* Inode, inode number(ino) --> inode fields, an inode is represented as a document

* Link, <parent ino, name> --> <child ino, timestmap>

* Space, a namespace of inodes & links

* Shard, partitioned by ino ranges (say 100 millions)

* InodeStore, the storage engine of inode shards

* ScalarIndex, the index of scalar fields

* VectorIndex, the index of embeddings


## Architecture

An InodeDB cluster has three server roles:

* Inoder - InodeServer, indexing & serving the inode shards

* Master, in charge of all the metadata - namespaces, inodeservers, inoderanges, et al.

* Merger, routing requests and merging responses from multiple shards

Every server provids endpoints via gRPC & RESTful API.

We define InodeDB as a specialized distributed document database, rather than a general-purpose key-value or relational database.

## Building Blocks

* Bluge
* Etcd
* Faiss
* gRPC
* Multiraft
* Rocksdb
* Prometheus

*/

package inodedb
