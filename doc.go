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

## Data Model

* Collections, Volumes, and Inodes.

* Inode, inode number(ino) --> inode fields, an inode is represented as a document

* Link, <parent ino, name> --> <child ino, timestmap>

* Collection, the logical 'type' of inodes/documents. While InodeDB is schemaless by design, all documents of a collection typically have a similar structure (although they can have different fields) and work for the same application.

* Volume, the physical 'container' of inodes, i.e., the partition, replication, and storage unit. Note volumes are created explicitly by the client.

* Scalar Index, the index of scalar fields

* Vector Index, the index of embeddings


## Architecture

An InodeDB cluster has two server roles:

* Inoder - InodeServer, serving the inode volumes

* Master, in charge of all the cluster-level metadata

Every server provids endpoints via gRPC & RESTful API.

We define InodeDB as a specialized distributed document database used as the metadata subsystem of cloud data lake and file or object storage, rather than a general-purpose key-value or relational database.

### Replication

multi-raft

### Storage

a volume has a single rocksdb instance

### CDC

the Change Data Capture


## Building Blocks

* Bluge
* Faiss
* gRPC
* Rocksdb
* Prometheus

*/

package inodedb
