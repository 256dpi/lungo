<img src="http://joel-github-static.s3.amazonaws.com/lungo/logo2.png" alt="Logo">

# lungo

[![Build Status](https://travis-ci.org/256dpi/lungo.svg?branch=master)](https://travis-ci.org/256dpi/lungo)
[![Coverage Status](https://coveralls.io/repos/github/256dpi/lungo/badge.svg?branch=master)](https://coveralls.io/github/256dpi/lungo?branch=master)
[![GoDoc](https://godoc.org/github.com/256dpi/lungo?status.svg)](http://godoc.org/github.com/256dpi/lungo)
[![Release](https://img.shields.io/github/release/256dpi/lungo.svg)](https://github.com/256dpi/lungo/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/256dpi/lungo)](http://goreportcard.com/report/256dpi/lungo)

**A MongoDB compatible embedded database and toolkit for Go.**

- [Installation](#installation)
- [Example](#example)
- [Introduction](#introduction)
- [Architecture](#architecture)
- [Features](#features)

## Installation

To get started, install the package using the go tool:

```bash
$ go get -u github.com/256dpi/lungo
```

## Example

The [example](https://github.com/256dpi/lungo/tree/master/example_test.go) test
shows a basic usage of the `mongo` compatible API.

## Introduction

The document oriented database MongoDB has become a widely used data store by
applications developed using the Go programming language. Both, the deprecated
`mgo` and the official `mongo` driver offer a sophisticated interface to connect
to a deployment, ingest and extract data using the various commands. While this
is enough for most projects, there are situations in which one thinks: "It would
be really cool if I could just do that in memory without hitting the server."

Lungo tries to address this need by re-implementing the data handling mechanics,
in Go to be used on the client side. This allows developers to pre or post 
process data in the application and relieving the server from work. One can think
of MongoDB query aware caches that are able to filter a subset of data and loading
more from the server if some documents are missing.

But we do not need to stop there: For example, working with Ruby on Rails in the
SQL ecosystem was always nice due to the availabilty of SQLite that allowed to
run tests without setting up a database or even run a small production apps using
just a file-backed SQLite database.

Lungo wants to offer a similar experience by implementing a full MongoDB 
compatible embeddable database that persists data in a single file. Here the
project aims to provide drop-in compatibility with the official Go driver by
implementing its full API. This way applications may use lungo for running their
tests or even low-write production deployments.

However, one thing this project does not try to do is building another
distributed database. MongoDB itself does a pretty good job at that already.

## Architecture

The codebase is divided into the packages `bsonkit`, `mongokit`, `dbkit` and
the main `lungo` package.

- The `bsonkit` package provides building blocks that extend the ones found in
the official `bson` package for handling BSON data. Its functions are mainly
useful to applications that need to inspect, compare, convert, transform,
clone, access and manipulate BSON data in memory.

- On top of that, the `mongokit` package provides the MongoDB data handling
algorithms and structures. Specifically, it implements the MongoDB querying,
update and sort algorithms as well as a btree based index for documents. All of
that is then bundled as a basic in memory collection of documents that offers a
standard CRUD interface.

- The `dbkit` package provides just some database centric utilities.

- Finally, the `lungo` package implements the embeddable database and the
`mongo` compatible driver. The heavy work is lifted by the engine and transaction
types which manage access to the basic mongokit collections. While both can be
used standalone, most users want to use the generic driver interface that can be
used with MongoDB deployments and lungo engines.

## Features

On high level, lungo provides the following features (unchecked features are
planned to be implemented):

- [x] CRUD, Index Management and Namespace Management
- [x] Single, Compound and Partial Indexes
- [ ] Index Based Sorting & Filtering
- [x] Sessions & Multi-Document Transactions
- [x] Oplog & Change Streams
- [x] Memory & Single File Store
- [ ] Projections
- [ ] Aggregation Pipeline

While the goal is to implement all MongoDB features in a compatible way, the
architectural difference has implications to some features. Furthermore, the
goal is to build an open and accessible codebase that favors simplicity.
Checkout the following sections for details on the implementation.

### CRUD, Index Management and Namespace Management

The driver supports all standard CRUD, index management and namespace management
methods that are also exposed by the official driver. However, to this date the
driver does not yet support any of the MongoDB commands that can be issued using
the `Database.RunCommand` method. Most unexported commands are related to query
planning, replication, sharding, user & role management features that we do not
plan to support. But, we eventually will support some of the administrative and
diagnostics commands e.g. `renameCollection` and `explain`.

The `mongokit.Match` function currently supports the following query operators:

- $and, $or, $nor, $not
- $eq, $gt, $lt, $gte, $lte, $ne
- $in, $nin, $exist, $type
- $all, $size, $elemMatch

And the `mongokit.Apply` function currently supports the following update
operators:

- $set, $setOnInsert, $unset, $rename
 - $inc, $mul, $max, $min
- $currentDate

### Single, Compound and Partial Indexes

The `mongokit.Index` type supports single field and compound indexes that
optionally enforce uniqueness or index a subset of documents using a partial
filter expression. Support for TTL indexes will be added shortly.

The more special multikey, geospatial, text and hashed indexes are not yet
supported and may be added later, while the deprecated sparse indexes will not.
The recently introduced collation feature as well as wildcard indexes are also
not yet supported.

### Index Based Sorting & Filtering

Indexes are currently only used to ensure uniqueness constraints and do not
support filtering and sorting. This will be added in the future together with
a proper iterator/cursor interface. 

### Sessions & Multi-Document Transactions

Lungo supports multi document transactions using a basic copy on write mechanism.
Every transaction will make a copy of the catalog and clone namespaces before
applying changes. After the new catalog has been written to disk, the transaction
is considered successful and the catalog replaced. Read only transactions are
allowed to run in parallel as they only serve as a snapshots. But write
transactions are run sequential. We assume write transactions to be fast and
therefore try to prevent abortions due to conflicts (pessimistic concurrency
control). The chosen approach might be changed in the future.

### Oplog & Change Streams

Similar to MongoDB, every CRUD change is also logged to the `local.oplog`
collection in the same format as consumed in MongoDB. This allows client,
database and collection change streams.

### Memory & Single File Store

The `lungo.Store` interface allows custom adapters that store the catalog to
various mediums. The builtin `MemoryStore` keeps all data in memory and the
`FileStore` writes all data atomically to a single BSON file. The interface may
get more sophisticated in the future to allow more efficient storing methods.
