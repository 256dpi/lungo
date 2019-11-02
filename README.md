# lungo

**A MongoDB compatible embedded database and toolkit for Go.**

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Features](#features)
- [Example](#example)
- [Installation](#installation)

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
- [x] Sessions & Multi-Document Transactions
- [x] Oplog & Change Streams
- [x] Memory & Single File Store
- [ ] Projections
- [ ] Aggregation Pipeline

While the goal is to implement all MongoDB features in a compatible way, the
architectural difference has implications to some features. Furthermore, the
goal is to build an open and accessible codebase that favors simplicity. Please
checkout the sections at the end for details on the implementation.

## Example

The [example](https://github.com/256dpi/lungo/tree/master/example_test.go) test
show a basic usage of the `mongo` compatible API.

## Installation

To get started, install the package using the go tool:

```bash
$ go get -u github.com/256dpi/lungo
```

## CRUD, Index Management and Namespace Management

The driver supports all standard CRUD, index management and namespace management
methods that are also exposed by the official driver. However, to this date the
driver does not yet support any of the MongoDB commands that can be issued using
