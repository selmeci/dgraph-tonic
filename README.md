# A async rust client for dgraph

[![Build Status](https://travis-ci.org/selmeci/dgraph-tonic.svg?branch=master)](https://travis-ci.org/selmeci/dgraph-tonic)
[![Latest Version](https://img.shields.io/crates/v/dgraph-tonic.svg)](https://crates.io/crates/dgraph-tonic)
[![Docs](https://docs.rs/dgraph-tonic/badge.svg)](https://docs.rs/dgraph-tonic)

Dgraph async Rust client which communicates with the server using [gRPC](https://grpc.io/)  build with [Tonic](https://github.com/hyperium/tonic).

Before using this client, it is highly recommended to go through [tour.dgraph.io] and [docs.dgraph.io] to understand how to run and work with Dgraph.

[docs.dgraph.io]: https://docs.dgraph.io
[tour.dgraph.io]: https://tour.dgraph.io

## Table of contents

- [Installation](#install)
- [Supported Versions](#supported-versions)
- [Using a client](#using-a-client)
  - [Create a client](#create-a-client)
  - [Alter the database](#alter-the-database)
  - [Create a transaction](#create-a-transaction)
  - [Run a mutation](#run-a-mutation)
  - [Run a query](#run-a-query)
  - [Running an Upsert: Query + Mutation](#running-an-upsert-query--mutation)
  - [Running a Conditional Upsert](#running-a-conditional-upsert)
  - [Commit a transaction](#commit-a-transaction)
- [Integration tests](#integration-tests)
- [Contributing](#contributing)

## Installation

`dgraph-tonic` is available on crates.io. Add the following dependency to your `Cargo.toml`.

```toml
[dependencies]
dgraph-tonic = "0.3"
```

Default feature is `dgraph-1-1`. 

If you want use Dgraph v1.0.x, add this dependency:

```toml
[dependencies]
dgraph-tonic = { version = "0.3", features = ["dgraph-1-0"], default-features = false }
```

## Supported Versions

Depending on the version of Dgraph that you are connecting to, you will have to use a different feature of this client (*dgraph-1-0* is default version).

| Dgraph version |      feature      |
|:--------------:|:-----------------:|
|     1.0.X      |    *dgraph-1-0*   |
|     1.1.X      |    *dgraph-1-1*   |
|     1.2.X      |    *dgraph-1-1*   |
|    20.03.0     |    *dgraph-1-1*   |

Note: Only API breakage from **dgraph-1-0* to *dgraph-1-1* is in the function `MutatedTxn.mutate()`. This function returns a `Assigned` type in *dgraph-1-0* but a `Response` type in *dgraph-1-1*.

## Using a client

### Create a client

`Client` object can be initialised by passing a vector of `tonic::transport::Endpoints`. Connecting to multiple Dgraph servers in the same cluster allows for better distribution of workload.

The following code snippet shows it with just one endpoint.

```rust
let client = Client::new(vec!["http://127.0.0.1:19080"]).await?;
```

Alternatively, secure client can be used:

```rust
let client = Client::new_with_tls_client_auth(
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
let read_only = client.read_only();
let best_effort = client.best_effort();
let mutated = client.mutated();
```

Only for Mutated transaction must be always called `txn.dicard().await?` or `txn.commit().await?` function before txn variable is dropped.

### Run a mutation

`txn.mutate(mu).await?` runs a mutation. It takes in a `Mutation` object. You can set the data using JSON or RDF N-Quad format. There exist helper functions for JSON format (`mu.set_set_json(), mu.set_delete_json()`)

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
};

let mut mu = Mutation::new();
mu.set_set_json(&p)?;

let mut txn = client.new_mutated_txn();
let assigned = txn.mutate(mu).await.expect("failed to create data");
```

Note: Only API breakage from *dgraph-1-0* to *dgraph-1-1* is in the function `MutatedTxn.mutate()`. This function returns a `Assigned` type in *dgraph-1-0* but a `Response` type in *dgraph-1-1*.

Sometimes, you only want to commit a mutation, without querying anything further. In such cases, you can use `txn.mutate_and_commit_now(mu)` to indicate that the mutation must be immediately committed. Txn object is being consumed in this case.

`Mutation::with_ignored_index_conflict()` can be applied on a `Mutation` object to not run conflict detection over the index, which would decrease the number of transaction conflicts and aborts. However, this would come at the cost of potentially inconsistent upsert operations. This flag is avaliable only in *dgraph-1-0*.

### Run a query

You can run a query by calling `txn.query(q)`. You will need to pass in a GraphQL+- query string. If you want to pass an additional map of any variables that you might want to set in the query, call `txn.query_with_vars(q, vars)` with the variables map as second argument.

Let's run the following query with a variable \$a:

```console
query all($a: string) {
  all(func: eq(name, $a))
  {
    uid
    name
  }
}
```

`Response` provides function `try_into()` which can be used for transforming returned JSON into coresponding struct object which implements serde `Deserialize` traits.

```rust
#[derive(Deserialize, Debug)]
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

let resp: Response = client.new_read_only_txn().query_with_vars(q, vars).await.expect("query");
let persons: Persons = resp.try_into().except("Persons");
println!("Persons: {:?}", persons);
```

When running a schema query, the schema response is found in the `Schema` field of `Response`.

```rust
let q = r#"schema(pred: [name]) {
  type
  index
  reverse
  tokenizer
  list
  count
  upsert
  lang
}"#.to_string();

let resp = client.new_read_only_txn().query(q).await?;
println!("{:#?}", resp.schema);
```

### Running an Upsert: Query + Mutation

Avaibale since `dgraph-1-1`.

The `txn.upsert(query, mutation)` function allows you to run upserts consisting of one query and one or more mutations. Query variables could be defined with `txn.upsert_with_vars(query, vars, mutation)`. Transaction in upsert is commited.

To know more about upsert, we highly recommend going through the docs at https://docs.dgraph.io/mutations/#upsert-block.

```rust
let q = r#"
  query {
      user as var(func: eq(email, "wrong_email@dgraph.io"))
  }"#;

let mut mu = Mutation::new();
mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);

let txn = client.new_mutated_txn();
// Upsert: If wrong_email found, update the existing data or else perform a new mutation.
let response = txn.upsert(q, mu).await.expect("failed to upsert data");

```

You can upsert with one mutation or vector of mutations.

### Running a Conditional Upsert

Avaibale since `dgraph-1-1`.

The upsert block allows specifying a conditional mutation block using an `@if` directive. The mutation is executed only when the specified condition is true. If the condition is false, the mutation is silently ignored.

See more about Conditional Upsert [Here](https://docs.dgraph.io/mutations/#conditional-upsert).

```rust
let q = r#"
  query {
      user as var(func: eq(email, "wrong_email@dgraph.io"))
  }"#;

let mut mu = Mutation::new();
mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
mu.set_cond("@if(eq(len(user), 1))");

let txn = client.new_mutated_txn();
let response = txn.upsert(q, vec![mu]).await.expect("failed to upsert data");
```

### Commit a transaction

A mutated transaction can be committed using the `txn.commit()` method. If your transaction consisted solely of calls to `txn.query` or `txn.query_with_vars`, and no calls to `txn.mutate`, then calling `txn.commit` is not necessary.

An error will be returned if other transactions running concurrently modify the same data that was modified in this transaction. It is up to the user to retry transactions when they fail.

```rust
let mut txn = client.new_mutated_txn();
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

Since we are working with a database, tests also need to be run in a single thread to prevent aborts. Feature flags are used depending on version of Dgraph you are using. Eg.:

```bash
cargo test --no-default-features --features dgraph-1-1 -- --test-threads=1
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
