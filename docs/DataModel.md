# Data Model

InodeDB is a specialized distributed system as the metadata service for large-scale unstructured data.

It was motivated by CubeFS metadata subsystem design, and will be used as the next-gen metadata management.

CubeFS' volume/partition/inode/dentry => InodeDB's space/shard/item/link

## core concepts

* space
* item
* ino
* field
* link

## search

* got by ino
* ordered links
* scalar fields
* text embeddings


