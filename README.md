# A async rust client for dgraph

[![Build Status](https://travis-ci.org/selmeci/dgraph-tonic.svg?branch=master)](https://travis-ci.org/selmeci/dgraph-tonic)
[![Latest Version](https://img.shields.io/crates/v/dgraph-tonic.svg)](https://crates.io/crates/dgraph-tonic)
[![Docs](https://docs.rs/dgraph-tonic/badge.svg)](https://docs.rs/dgraph-tonic)

dGraph async Rust client which communicates with the server using [gRPC](https://grpc.io/)  build with [Tonic](https://github.com/hyperium/tonic).

Before using this client, it is highly recommended to go through [tour.dgraph.io] and [docs.dgraph.io] to understand how to run and work with Dgraph.

**Only dGraph version 1.0.x is now supported.**

[docs.dgraph.io]: https://docs.dgraph.io
[tour.dgraph.io]: https://tour.dgraph.io

## Table of contents

- [Installation](#install)
- [Using a client](#using-a-client)
  - [Create a client](#create-a-client)
  - [Alter the database](#alter-the-database)
  - [Create a transaction](#create-a-transaction)
  - [Run a mutation](#run-a-mutation)
  - [Run a query](#run-a-query)
  - [Commit a transaction](#commit-a-transaction)
- [Integration tests](#integration-tests)
- [Contributing](#contributing)

## Installation

`dgraph-tonic` is available on crates.io. Add the following dependency to your `Cargo.toml`.

```toml
[dependencies]
dgraph = "0.1"
```

## Using a client

### Create a client

`Client` object can be initialised by passing a vector of `tonic::transport::Endpoints`. Connecting to multiple dGraph servers in the same cluster allows for better distribution of workload.

The following code snippet shows it with just one endpoint.

```rust
let client = Client::new(vec!["http://127.0.0.1:19080"]).await?;
```

Alternatively, secure client can be used:

```rust
let client = Client::new_with_tls_client_auth(
    "dgraph.io"
    vec!["https://dgraph.io"],
    "/path/ca.crt",
    "/path/client.crt",
    "/path/client.key",
).await?;
```

All certs must be in `PEM` format.

### Alter the database

To set the schema, create an instance of `dgraph::Operation` and use the `Alter` endpoint.

```rust
let op = Operation {
    schema: "name: string @index(exact) .".into(),
    ..Default::default()
};
let response = client.alter(op).await?;
```

`Operation` contains other fields as well, including `DropAttr` and `DropAll`. `DropAll` is useful if you wish to discard all the data, and start from a clean slate, without bringing the instance down. `DropAttr` is used to drop all the data related to a predicate.

### Create a transaction

Transaction is modeled with [The Typestate Pattern in Rust](http://cliffle.com/blog/rust-typestate/). The typestate pattern is an API design pattern that encodes information about an object's run-time state in its compile-time type. This principle allows us to identify some type of errors, like mutation in read only transaction, during compilation. Transaction types are:

- *Default*: can be transformed into ReadOnly, BestEffort, Muated. Can perform `query` and `query_with_vars` actions.
- *ReadOnly*: useful to increase read speed because they can circumvent the usual consensus protocol. Can perform `query` and `query_with_vars` actions.
- *BestEffort*: Read-only queries can optionally be set as best-effort. Using this flag will ask the Dgraph Alpha to try to get timestamps from memory on a best-effort basis to reduce the number of outbound requests to Zero. This may yield improved latencies in read-bound workloads where linearizable reads are not strictly needed. Can permorm `query` and `query_with_vars` actions.
- *Mutated*: can perform all actions as default transaction and can modify data in DB. Can be created only from default transaction.

Client provides several factory methods for transactions. Create new transaction incurs no network overhead.

```rust
let txn = client.new_txn();
let read_only = txn.read_only();
let best_effort = txn.best_effort();
let mutated = txn.mutated();
```

Only for Mutated transaction must be always called `txn.dicard().await?` or `txn.commit().await?` function before txn variable is dropped.

### Run a mutation

`txn.mutate(mu).await?` runs a mutation. It takes in a `Mutation` object. You can set the data using JSON or RDF N-Quad format. There exist helper functions for JSON format (`mu.with_set_json(), mu.with_delete_json()`)

Example:

```rust
#[derive(Serialize, Deserialize, Default, Debug)]
struct Person {
  uid: String,
  name: String,
}

let p = Person {
  uid:  "_:alice".into(),
  name: "Alice".into(),
}

let mu = Mutation::new().with_set_json(&p)?;

let txn = client.new_mutated_txn();
let assigned = txn.mutate(mu).await.expect("failed to create data");
```

Sometimes, you only want to commit a mutation, without querying anything further. In such cases, you can use `txn.mutate_and_commit_now(mu)` to indicate that the mutation must be immediately committed. Txn object is being consumed in this case.

### Run a query

You can run a query by calling `txn.query(q)`. You will need to pass in a GraphQL+- query string. If you want to pass an additional map of any variables that you might want to set in the query, call `txn.query_with_vars(q, vars)` with the variables map as second argument.

Let's run the following query with a variable \$a:

```rust
#[derive(Serialize, Deserialize, Default, Debug)]
struct Persons {
  all: Vec<Person>
}

let q = r#"query all($a: string) {
    all(func: eq(name, $a)) {
      uid
      name
    }
  }"#;

let mut vars = HashMap::new();
vars.insert("$a", "Alice");

let resp = client.new_readonly_txn().query_with_vars(q, vars).await.expect("query");
let persons: Persons = resp.try_into().except("Persons");
println!("Persons: {:?}", persons);
```

### Commit a transaction

A mutated transaction can be committed using the `txn.commit()` method. If your transaction consisted solely of calls to `txn.query` or `txn.query_with_vars`, and no calls to `txn.mutate`, then calling `txn.commit` is not necessary.

An error will be returned if other transactions running concurrently modify the same data that was modified in this transaction. It is up to the user to retry transactions when they fail.

```rust
let txn = client.new_mutated_txn();
// Perform some queries and mutations.

let res = txn.commit().await;
if res.is_err() {
  // Retry or handle error
}
```

## Integration tests

Tests require Dgraph running on `localhost:19080`. For the convenience there are two `docker-compose.yaml` files, depending on Dgraph you are testing against, prepared in the root directory:

```bash
docker-compose -f docker-compose-1-X.yaml up -d
```

Since we are working with a database, tests also need to be run in a single thread to prevent aborts.

```bash
cargo test -- --test-threads=1
```

## Contributing

Contributions are welcome. Feel free to raise an issue, for feature requests, bug fixes and improvements.

## Release checklist

These have to be done with both Dgraph 1.0 and Dgraph 1.1+:

- Run tests
- Try examples

Update the version and publish crate:

- Update tag in Cargo.toml
- Update tag in README.md
- `git tag v0.X.X`
- `git push origin v0.X.X`
- Write release log on GitHub
- `cargo publish`
