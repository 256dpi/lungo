<img src="http://joel-github-static.s3.amazonaws.com/lungo/logo2.png" alt="Logo" width="400"/>

# lungo

[![Test](https://github.com/256dpi/lungo/actions/workflows/test.yml/badge.svg)](https://github.com/256dpi/lungo/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/256dpi/lungo?status.svg)](https://godoc.org/github.com/256dpi/lungo)
[![Release](https://img.shields.io/github/release/256dpi/lungo.svg)](https://github.com/256dpi/lungo/releases)

**A MongoDB compatible embeddable database and toolkit for Go.**

- [Installation](#installation)
- [Example](#example)
- [Motivation](#motivation)
- [Architecture](#architecture)
- [Features](#features)
- [License](#license)

## Installation

To get started, install the package using the go tool:

```bash
$ go get -u github.com/256dpi/lungo
```

## Example

This [example](https://github.com/256dpi/lungo/tree/master/example_test.go)
shows a basic usage of the `mongo` compatible API.

## Motivation

The document-oriented database MongoDB has become a widely used data store for
many applications developed with the Go programming language. Both, the deprecated
`mgo` and the official `mongo` driver offer a sophisticated interface to connect
to a deployment and ingest and extract data using various commands. While this
is enough for most projects, there are situations in which one thinks: "It would
be cool if I could just do that in memory without asking the server."

Lungo tries to address this need by re-implementing the data handling mechanics
in Go to be used on the client-side. This allows developers to pre- or
post-process data in the application relieving the server. For example,
applications may utilize this functionality to cache documents and query them
quickly in memory.

But we do not need to stop there. Many developers coming from the SQL ecosystem
enjoy working with SQLite as a simple alternative to bigger SQL databases. It
allows running tests without setting up a database or even small production
apps that write their data to a single backed-up file.

Lungo wants to offer a similar experience by implementing a full MongoDB
compatible embeddable database that persists data in a single file. The
project aims to provide drop-in compatibility with the API exported by the
official Go driver. This way, applications may use lungo for running their
tests or even low-write production deployments without big code changes.

However, one thing this project does not try to do is build another
distributed database. MongoDB itself does a pretty good job at that already.

## Architecture

The codebase is divided into the packages `bsonkit`, `mongokit`, `dbkit` and
the main `lungo` package.

- The `bsonkit` package provides building blocks that extend the ones found in
the official `bson` package for handling BSON data. Its functions are mainly
useful to applications that need to inspect, compare, convert, transform,
clone, access, and manipulate BSON data directly in memory.

- On top of that, the `mongokit` package provides the MongoDB data handling
algorithms and structures. Specifically, it implements the MongoDB querying,
update, and sort algorithms as well as a btree based index for documents. All of
that is then bundled as a basic in-memory collection of documents that offers a
familiar CRUD interface.

- The `dbkit` package provides database-centric utilities e.g. atomic file write.

- Finally, the `lungo` package implements the embeddable database and the
`mongo` compatible driver. The heavy work is done by the engine and transaction
types that manage access to the basic `mongokit.Collection` instances. While both
can be used standalone, most users want to use the generic driver interface that
can be used with MongoDB deployments and lungo engines.

## Features

On a high level, lungo provides the following features (unchecked features are
planned to be implemented):

- [x] CRUD, Index Management and Namespace Management
- [x] Single, Compound and Partial Indexes
- [ ] Index Supported Sorting & Filtering
- [x] Sessions & Multi-Document Transactions
- [x] Oplog & Change Streams
- [ ] Aggregation Pipeline
- [x] Memory & Single File Store
- [x] GridFS

While the goal is to implement all MongoDB features in a compatible way, the
architectural difference has implications on some of the features. Furthermore,
the goal is to build an open and accessible codebase that favors simplicity.
Check out the following sections for details on the implementation.

### CRUD, Index Management and Namespace Management

The driver supports all standard CRUD, index management and namespace management
methods that are also exposed by the official driver. However, to this date, the
driver does not yet support any of the MongoDB commands that can be issued using
the `Database.RunCommand` method. Most unexported commands are related to query
planning, replication, sharding, and user and role management features that we
do not plan to support. However, we eventually will support some of the
administrative and diagnostics commands e.g. `renameCollection` and `explain`.

Leveraging the `mongokit.Match` function, lungo supports the following query
operators:

- `$and`, `$or`, `$nor`, (`$not`)
- `$eq`, `$gt`, `$lt`, `$gte`, `$lte`, `$ne`
- (`$in`), (`$nin`), `$exist`, `$type`
- `$jsonSchema`, `$all`, `$size`, `$elemMatch`

And the `mongokit.Apply` function currently supports the following update
operators:

- `$set`, `$setOnInsert`, `$unset`, `$rename`
- `$inc`, `$mul`, `$max`, `$min`, (`$push`)
- `$pop`, `$currentDate`, `$[]`, `$[<identifier>]`

Finally, the `mongokit.Project` function currently supports the following
projection operators:

- `$slice`

Operators in braces are only partially supported, see comments in code.

### Single, Compound and Partial Indexes

The `mongokit.Index` type supports single field and compound indexes that
optionally enforce uniqueness or index a subset of documents using a partial
filter expression. Single field indexes also support the automated expiry of
documents aka. TTL indexes.

The more advanced multikey, geospatial, text, and hashed indexes are not yet
supported and may be added later, while the deprecated sparse indexes will not.
The recently introduced collation feature, as well as wildcard indexes, are also
subject to future development.

### Index Supported Sorting & Filtering

Indexes are currently only used to ensure uniqueness constraints and do not
support filtering and sorting. This will be added in the future together with
support for the `explain` command to debug the generated query plan.

### Sessions & Multi-Document Transactions

Lungo supports multi-document transactions using a basic copy on write mechanism.
Every transaction will make a copy of the catalog and clone namespaces before
applying changes. After the new catalog has been written to disk, the transaction
is considered successful and the catalog replaced. Read-only transactions are
allowed to run in parallel as they only serve as snapshots. But write
transactions are run sequentially. We assume write transactions to be fast and
therefore try to prevent abortions due to conflicts (pessimistic concurrency
control). The chosen approach might be changed in the future.

### Oplog & Change Streams

Similar to MongoDB, every CRUD change is also logged to the `local.oplog`
collection in the same format as consumed by change streams in MongoDB. Based on
that, change streams can be used in the same way as with MongoDB replica sets.

### Memory & Single File Store

The `lungo.Store` interface enables custom adapters that store the catalog to
various mediums. The built-in `MemoryStore` keeps all data in memory while the
`FileStore` writes all data atomically to a single BSON file. The interface may
get more sophisticated in the future to allow more efficient storing methods.

### GridFS

The `lungo.Bucket`, `lungo.UploadStream` and `lungo.DownloadStream` provide a
GridFS implementation similar to the one found in the `gridfs` package of the
official Go driver. However, some improvements have been made while
re-implementing the package:

- Support for sessions via the `context.Context` parameter in all `lungo.Bucket`
methods.
- The `lungo.DowloadStream` implements the `io.Seeker` interface for convenient
range queries on the file contents.
- A non-standard "tracking" mode in which in-progress uploads and deletions are
tracked by storing a document in an additional "markers" collection. If enabled,
uploads can be suspended and resumed later and must be explicitly claimed. All
unclaimed uploads and not fully deleted files can be cleaned up.

## License

The MIT License (MIT)

Copyright (c) 2019 Joël Gähwiler
