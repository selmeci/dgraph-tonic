# An async/sync rust client for Dgraph DB

[![Build Status](https://travis-ci.org/selmeci/dgraph-tonic.svg?branch=master)](https://travis-ci.org/selmeci/dgraph-tonic)
[![Latest Version](https://img.shields.io/crates/v/dgraph-tonic.svg)](https://crates.io/crates/dgraph-tonic)
[![Docs](https://docs.rs/dgraph-tonic/badge.svg)](https://docs.rs/dgraph-tonic)
[![Coverage Status](https://coveralls.io/repos/github/selmeci/dgraph-tonic/badge.svg?branch=master)](https://coveralls.io/github/selmeci/dgraph-tonic?branch=master)

Dgraph async/sync Rust client which communicates with the server using [gRPC](https://grpc.io/)  build with [Tonic](https://github.com/hyperium/tonic).

Before using this client, it is highly recommended to go through [tour.dgraph.io] and [docs.dgraph.io] to understand how to run and work with Dgraph.

[docs.dgraph.io]: https://docs.dgraph.io
[tour.dgraph.io]: https://tour.dgraph.io

## Table of contents

- [Installation](#install)
- [Supported Versions](#supported-versions)
- [Using a client](#using-a-client)
  - [Create a client](#create-a-client)
  - [Create a TLS client](#create-a-tls-client)
  - [Create a Sync client](#create-a-sync-client)
  - [Alter the database](#alter-the-database)
  - [Create a transaction](#create-a-transaction)
  - [Run a mutation](#run-a-mutation)
  - [Run a query](#run-a-query)
  - [Running an Upsert: Query + Mutation](#running-an-upsert-query--mutation)
  - [Running a Conditional Upsert](#running-a-conditional-upsert)
  - [Commit a transaction](#commit-a-transaction)
  - [Access Control Lists](#access-control-lists)
  - [Check version](#check-version)
- [Examples](#examples)
- [Integration tests](#integration-tests)
- [Contributing](#contributing)
- [Thanks to](#thanks-to)

## Installation

`dgraph-tonic` is available on crates.io. Add the following dependency to your `Cargo.toml`.

```toml
[dependencies]
dgraph-tonic = "0.6"
```

Default feature is `dgraph-1-1`.

All avaiable features can be activeted with:

```toml
[dependencies]
dgraph-tonic = {version = "0.6", features = ["all"]}
```

If you want to use Dgraph v1.0.x, add this dependency:

```toml
[dependencies]
dgraph-tonic = { version = "0.6", features = ["dgraph-1-0"], default-features = false }
```

Supported features:

- *acl*: Enable client with authentification.
- *all*: enable tls, acl and sync features with dgraph-1-1
- *dgraph-1-0*: Enable client for Dgraph v1.0.x
- *dgraph-1-1*: Enable client for Dgraph v1.1.x and v20.0.x
- *tls*: Enable secured TlsClient
- *sync*: Enable synchronous Client

## Supported Versions

Depending on the version of Dgraph that you are connecting to, you will have to use a different feature of this client (*dgraph-1-0* is default version).

| Dgraph version |      feature      |
|:--------------:|:-----------------:|
|     1.0.X      |    *dgraph-1-0*   |
|     1.1.X      |    *dgraph-1-1*   |
|     1.2.X      |    *dgraph-1-1*   |
|    20.03.X     |    *dgraph-1-1*   |

Note: Only API breakage from **dgraph-1-0* to *dgraph-1-1* is in the function `MutatedTxn.mutate()`. This function returns a `Assigned` type in *dgraph-1-0* but a `Response` type in *dgraph-1-1*.

## Using a client

### Create a client

`Client` object can be initialised by passing a one endpoint uri or vector of endpoints uris. Connecting to multiple Dgraph servers in the same cluster allows for better distribution of workload.

The following code snippet shows it with just one endpoint.

```rust
let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
```

or you can initialize new client with a multiple endpoints. Client cannot be create with empty endpoints vector.

```rust
let client = Client::new(vec!["http://127.0.0.1:9080","http://127.0.0.1:19080"]).expect("Dgraph client");
```

### Create a TLS client

Alternatively, secure tls client is avaible in `tls` feature:

```toml
[dependencies]
dgraph-tonic = { version = "0.6", features = ["tls"] }
```

```rust
use dgraph_tonic::TlsClient;

#[tokio::main]
async fn main() {
  let server_root_ca_cert = tokio::fs::read("path/to/ca.crt").await.expect("CA cert");
  let client_cert = tokio::fs::read("path/to/client.crt").await.expect("Client cert");
  let client_key = tokio::fs::read("path/to/ca.key").await.expect("Client key");
  let client = TlsClient::new(
    vec!["http://192.168.0.10:19080", "http://192.168.0.11:19080"],
    server_root_ca_cert,
    client_cert,
    client_key)
  .expect("Dgraph TLS client");
}
```

All certs must be in `PEM` format.

### Create a Sync client

Alternatively, synchronous clients (Tls, Acl) are avaible with `sync` feature in `dgraph_tonic::sync` module:

```toml
[dependencies]
dgraph-tonic = { version = "0.6", features = ["sync"] }
```

```rust
use dgraph_tonic::sync::{Mutate, Client};

fn main() {
  let p = Person {
    uid:  "_:alice".into(),
    name: "Alice".into(),
  };
  let mut mu = Mutation::new();
  mu.set_set_json(&p).expect("JSON");
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_mutated_txn();
  let response = txn.mutate(mu).expect("Mutated");
  txn.commit().expect("Transaction is commited");
}
```

All sync clients support same functionalities as async versions.

### Alter the database

To set the schema, create an instance of `Operation` and use the `Alter` endpoint.

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let op = Operation {
    schema: "name: string @index(exact) .".into(),
    ..Default::default()
  };
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let response = client.alter(op).await.expect("Schema set");
  //you can set schema directly with
  client.set_schema("name: string @index(exact) .").await.expect("Schema is not updated");
}
```

`Operation` contains other fields as well, including `DropAttr` and `DropAll`. `DropAll` is useful if you wish to discard all the data, and start from a clean slate, without bringing the instance down. `DropAttr` is used to drop all the data related to a predicate.

If you want to drop all data in DB, you can use:

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  client.drop_all().await.expect("Data not dropped");
}
```


### Create a transaction

Transaction is modeled with [The Typestate Pattern in Rust](http://cliffle.com/blog/rust-typestate/). The typestate pattern is an API design pattern that encodes information about an object's run-time state in its compile-time type. This principle allows us to identify some type of errors, like mutation in read only transaction, during compilation. Transaction types are:

- *Default*: can be transformed into ReadOnly, BestEffort, Mutated. Can perform `query` and `query_with_vars` actions.
- *ReadOnly*: useful to increase read speed because they can circumvent the usual consensus protocol. Can perform `query` and `query_with_vars` actions defined in `Query` trait.
- *BestEffort*: Read-only queries can optionally be set as best-effort. Using this flag will ask the Dgraph Alpha to try to get timestamps from memory on a best-effort basis to reduce the number of outbound requests to Zero. This may yield improved latencies in read-bound workloads where linearizable reads are not strictly needed. Can permorm `query` and `query_with_vars` actions.
- *Mutated*: can perform all actions as default transaction and can modify data in DB. Can be created only from default transaction.

Client provides several factory methods for transactions. These operations incur no network overhead.

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let txn = client.new_txn();
  let read_only = client.new_read_only_txn();
  let best_effort = client.new_best_effort_txn();
  let mutated = client.new_mutated_txn();
}

```

Only for Mutated transaction must be always called `txn.dicard().await?` or `txn.commit().await?` function before txn variable is dropped.

Because internal state of transaction depends on used variant (plain, tls, plain + acl, tls + acl x read only, mutated) you should use traits in your structures for transaction parameters.
```rust
struct MyTxn<Q: Query, M: Mutate> {
    readonly_txn: Q,
    mutated_txn: M,
   
}
```

or as returning parameter from function
```rust
fn my_query_operation() -> impl Query {
    //your code
}

fn my_mutation_operation() -> impl Mutate {
    //your code
}
```

If you cannot use generics or traits object you can use predefined exported transaction types: *Txn, TxnReadOnly, TxnBestEffort, TxnMutated, TxnTls, TxnTlsReadOnly, TxnTlsBestEffort, TxnTlsMutated, TxnAcl, TxnAclReadOnly, TxnAclBestEffort, TxnAclMutated, TxnAclTls, TxnAclTlsReadOnly, TxnAclTlsBestEffort, TxnAclTlsMutated*

### Run a mutation

`txn.mutate(mu).await?` runs a mutation. It takes in a `Mutation` object. You can set the data using JSON or RDF N-Quad format. There exist helper functions for JSON format (`mu.set_set_json(), mu.set_delete_json()`). All mutation operations are defined in `Mutate` trait.

Example:

```rust
use dgraph_tonic::{Mutate, Client};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Default, Debug)]
struct Person {
  uid: String,
  name: String,
}

#[tokio::main]
async fn main() {
  let p = Person {
    uid:  "_:alice".into(),
    name: "Alice".into(),
  };
  let mut mu = Mutation::new();
  mu.set_set_json(&p).expect("JSON");
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_mutated_txn();
  let response = txn.mutate(mu).await.expect("Mutated");
  txn.commit().await.expect("Transaction is commited");
}
```

Note: Only API breakage from *dgraph-1-0* to *dgraph-1-1* is in the function `MutatedTxn.mutate()`. This function returns a `Assigned` type in *dgraph-1-0* but a `Response` type in *dgraph-1-1*.

Sometimes, you only want to commit a mutation, without querying anything further. In such cases, you can use `txn.mutate_and_commit_now(mu)` to indicate that the mutation must be immediately committed. Txn object is being consumed in this case.

In `dgraph-1-0` a `Mutation::with_ignored_index_conflict()` can be applied on a `Mutation` object to not run conflict detection over the index, which would decrease the number of transaction conflicts and aborts. However, this would come at the cost of potentially inconsistent upsert operations. This flag is avaliable only in *dgraph-1-0*.

#### How to get assigned UIDs from response

You can [specify your own key for returned uid](https://dgraph.io/docs/mutations/#setting-literal-values) like:

```rust
use dgraph_tonic::{Mutate, Client};
use serde::{Serialize, Deserialize};
use serde_json::json;

#[tokio::main]
async fn main() {
  let p = json!({
    "uid": "_:diggy",
    "name": "diggy",
    "food": "pizza"
  });
  let mut mu = Mutation::new();
  mu.set_set_json(&p).expect("JSON");
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_mutated_txn();
  let response = txn.mutate(mu).await.expect("Mutated");
  txn.commit().await.expect("Transaction is commited");
  println!("{:?}", response.uids);
}
```

Assigned uids are returned in response property `uids`. Exmple printlns something like:

```json
{"diggy": "0xc377"}

```

### Run a query

You can run a query by calling `txn.query(q)`. You will need to pass in a GraphQL+- query string. If you want to pass an additional map of any variables that you might want to set in the query, call `txn.query_with_vars(q, vars)` with the variables map as second argument. All query operations are defined in `Query` trait.

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
use dgraph_tonic::{Client, Query};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Person {
  uid: String,
  name: String,
}

#[derive(Deserialize, Debug)]
struct Persons {
  all: Vec<Person>
}

#[tokio::main]
async fn main() {
  let q = r#"query all($a: string) {
    all(func: eq(name, $a)) {
      uid
      name
    }
  }"#;
  let mut vars = HashMap::new();
  vars.insert("$a", "Alice");
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_read_only_txn();
  let response = txn.query_with_vars(q, vars).await.expect("Response");
  let persons: Persons = resp.try_into().except("Persons");
  println!("Persons: {:?}", persons);
}
```

When running a schema query, the schema response is found in the `Schema` field of `Response`.

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let q = r#"schema(pred: [name]) {
    type
    index
    reverse
    tokenizer
    list
    count
    upsert
    lang
  }"#;
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_read_only_txn();
  let response = txn.query(q).await.expect("Response");
  println!("{:#?}", response.schema);
}
```

#### Stream/Iterator

This functions are avaiable in `experimental` feature.

Sometimes you cannot fetch all desired data from query at once because it can be huge response which can results into gRPC error, etc. . In this case you can transfrom your query into stream `Stream<Item = Result<T, Error>>` (or iterator in sync mode) which will query your data in a chunks with defined capacity. Exist some limitations how you must defined your query:

1. your query must accept `$first: string, $offset: string` input arguments.
2. items for stream/iterator must be returned in block with `items` name.

Stream/Iterator ends when:

- query returns no items,
- query returns error which is last item returned from stream,
- query response cannot be deserialized into `Vec<T>`.

```rust
use std::collections::HashMap;
use failure::Error;
use futures::pin_mut;
use futures::stream::StreamExt;
use dgraph_tonic::{Client, Response, Query};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Person {
  uid: String,
  name: String,
}

#[tokio::main]
async fn main() {
  let query = r#"query stream($first: string, $offset: string, $name: string) {
         items(func: eq(name, $name), first: $first, offset: $offset) {
             uid
             name
         }
     }"#;
  let mut vars = HashMap::new();
  vars.insert("$name", "Alice");
  let client = client().await;
  let stream = client.new_read_only_txn().into_stream_with_vars(query, vars, 100);
  pin_mut!(stream);
  let alices: Vec<Result<Person, Error>> = stream.collect().await;
}
```

If you don't want to specify input vars, you can call `client.new_read_only_txn().into_stream(query, 100)`.

_Sync_ read only transaction can be transformed into iterator with `client.new_read_only_txn().into_iter(query, 100)` and `client.new_read_only_txn().into_iter_with_vars(query, vars, 100)`

### Running an Upsert: Query + Mutation

Avaibale since `dgraph-1-1`.

The `txn.upsert(query, mutation)` function allows you to run upserts consisting of one query and one or more mutations. Query variables could be defined with `txn.upsert_with_vars(query, vars, mutation)`. If you would like to commit upsert operation you can use `txn.upsert_with_vars_and_commit_now()`. Txn object is being consumed in this case.

To know more about upsert, we highly recommend going through the docs at https://docs.dgraph.io/mutations/#upsert-block.

```rust
use dgraph_tonic::{Mutate, Client};

#[tokio::main]
async fn main() {
  let q = r#"
    query {
        user as var(func: eq(email, "wrong_email@dgraph.io"))
    }"#;
  let mut mu = Mutation::new();
  mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let txn = client.new_mutated_txn();
  // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
  let response = txn.upsert(q, mu).await.expect("Upserted data");
  tnx.commit().await.expect("Committed");
}
```

You can upsert with one mutation or vector of mutations. If you would like to commit upsert operation you can use `txn.upsert_and_commit_now()`. Txn object is being consumed in this case.

### Running a Conditional Upsert

Avaibale since `dgraph-1-1`.

The upsert block allows specifying a conditional mutation block using an `@if` directive. The mutation is executed only when the specified condition is true. If the condition is false, the mutation is silently ignored.

See more about Conditional Upsert [Here](https://docs.dgraph.io/mutations/#conditional-upsert).

```rust
use dgraph_tonic::{Client, Mutate};

#[tokio::main]
async fn main() {
  let q = r#"
    query {
        user as var(func: eq(email, "wrong_email@dgraph.io"))
    }"#;

  let mut mu = Mutation::new();
  mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
  mu.set_cond("@if(eq(len(user), 1))");

  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let txn = client.new_mutated_txn();
  let response = txn.upsert(q, vec![mu]).await.expect("Upserted data");
  tnx.commit().await.expect("Committed");
}
```

### Commit a transaction

A mutated transaction can be committed using the `txn.commit()` method. If your transaction consisted solely of calls to `txn.query` or `txn.query_with_vars`, and no calls to `txn.mutate`, then calling `txn.commit` is not necessary.

An error will be returned if other transactions running concurrently modify the same data that was modified in this transaction. It is up to the user to retry transactions when they fail.

```rust
use dgraph_tonic::{Client, Mutate};

#[tokio::main]
async fn main() {
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let mut txn = client.new_mutated_txn();
  // Perform some queries and mutations.

  //than commit
  let res = txn.commit().await;
  if res.is_err() {
    // Retry or handle error
  }
}
```

### Access Control Lists

This enterprise Dgraph feature which can be activated with:

```toml
[dependencies]
dgraph-tonic = { version = "0.6", features = ["acl"] }
```

[Access Control List (ACL)](https://dgraph.io/docs/enterprise-features/#access-control-lists) provides access protection to your data stored in Dgraph. When the ACL feature is turned on, a client must authenticate with a username and password before executing any transactions, and is only allowed to access the data permitted by the ACL rules.

Both, `Client` and `TlsClient` can be logged in with `login(user_id,password)` function. Original client is consumed and return instance of `AclClient` which allows token refreshing with `refresh_login()` function.

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let logged_in_client = client.login("groot", "password").await.expect("Logged in");
  // use client

  //than refresh token
  logged_in_client.refresh_login().await.except("Refreshed access token");
}

```

### Check version

```rust
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
  let version = client.check_version().await.expect("Version");
  println!("{:#?}", version);
}

```

Output:

```console
Version {
    tag: "v20.03.0",
}
```

## Examples

- [simple][]: Quickstart example of using dgraph-tonic.
- [tls][]: Example of using dgraph-tonic with a Dgraph cluster secured with TLS.

[simple]: ./examples/simple
[tls]: ./examples/tls

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

Run these commands, before you create pull request:

```bash
rustup component add rustfmt
cargo fmt
```

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

## Thanks to

[![eDocu]](https://www.edocu.com/)

[edocu]: ./eDocu_Blue.png