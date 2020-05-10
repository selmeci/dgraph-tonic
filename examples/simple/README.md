# Simple example project

Simple project demonstrating the use of [dgraph-tonic](https://github.com/selmeci/dgraph-tonic), the async RUST client for Dgraph.

## Running

### Start Dgraph server

You will need to install [Dgraph v1.1.0 or above](https://github.com/dgraph-io/dgraph/releases) and run it.

You can run the commands below to start a clean Dgraph server every time for testing and exploration.

First, create two separate directories for `dgraph zero` and `dgraph alpha`.

```sh
mkdir -p dgraphdata/zero dgraphdata/data
```

Then start `dgraph zero`:

```sh
cd dgraphdata/zero
rm -r zw; dgraph zero
```

Finally, start the `dgraph alpha`:

```sh
cd dgraphdata/data
rm -r p w; dgraph alpha --lru_mb=1024 --zero localhost:5080
```

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

## Run the sample code

For run:

```sh
cargo run
```

Your output should look something like this (uid values may be different):

```console
Created person named "Alice" with uid = 0x7569

All created nodes (map from blank node names to uids):
alice => 0x7569
dg.1447158641.7 => 0x756a
dg.1447158641.8 => 0x756b
dg.1447158641.9 => 0x756c

Number of people named "Alice": 1
Person {
    uid: "0x1",
    name: "Alice",
    age: 26,
    married: true,
    loc: Location {
        t: "Point",
        coordinates: [
            1.1,
            2.0,
        ],
    },
    dob: 1980-01-01T23:00:00Z,
    friend: [
        Friend {
            name: "Bob",
            age: 24,
        },
        Friend {
            name: "Charlie",
            age: 29,
        },
    ],
    school: [
        School {
            name: "Crown Public School",
        },
    ],
}

DONE!
```

You can explore the source code in the `main.rs` file.
