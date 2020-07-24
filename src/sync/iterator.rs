use std::collections::HashMap;
use std::hash::Hash;

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::client::ILazyClient;
use crate::sync::{Query, TxnReadOnlyType};

#[derive(Deserialize)]
struct Chunk<T> {
    items: Vec<T>,
}

struct IteratorState<C, T>
where
    C: ILazyClient,
    T: DeserializeOwned,
{
    txn: TxnReadOnlyType<C>,
    query: String,
    vars: HashMap<String, String>,
    items: Vec<T>,
    first: usize,
    offset: usize,
    error: bool,
    last_page: bool,
}

impl<C, T> IteratorState<C, T>
where
    C: ILazyClient,
    T: DeserializeOwned,
{
    fn new<Q, K, V>(txn: TxnReadOnlyType<C>, query: Q, vars: HashMap<K, V>, first: usize) -> Self
    where
        Q: Into<String>,
        K: Into<String> + Eq + Hash,
        V: Into<String>,
    {
        let mut vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        vars.insert(String::from("$first"), format!("{}", first));
        Self {
            txn,
            query: query.into(),
            vars,
            items: Vec::with_capacity(0),
            first,
            offset: 0,
            error: false,
            last_page: false,
        }
    }

    fn fetch_items(&mut self) -> Result<Vec<T>> {
        let mut vars = self.vars.to_owned();
        vars.insert(String::from("$offset"), format!("{}", self.offset));
        let mut chunk: Chunk<T> = self
            .txn
            .query_with_vars(self.query.to_owned(), vars)?
            .try_into_owned()?;
        chunk.items.reverse();
        if chunk.items.len() < self.first {
            self.last_page = true;
        }
        Ok(chunk.items)
    }
}

impl<C, T> Iterator for IteratorState<C, T>
where
    C: ILazyClient,
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error {
            return None;
        }
        if self.items.is_empty() && !self.last_page {
            match self.fetch_items() {
                Ok(items) => self.items = items,
                Err(err) => {
                    self.error = true;
                    return Some(Err(err));
                }
            }
        }
        if let Some(item) = self.items.pop() {
            self.offset += 1;
            Some(Ok(item))
        } else {
            None
        }
    }
}

impl<C: ILazyClient> TxnReadOnlyType<C> {
    ///
    /// Readonly transaction is transformed into iterator.
    ///
    /// Input `query` must accept **$first: string, $offset: string** arguments which are used for paginating.
    /// Iterator items must be returned in query block named **items**.
    ///
    /// # Return
    ///
    /// Iterator contains deserialized items returned from query.
    /// Iterato item is Ok(T) if **items** query data can be serialized into Vec<T>.
    ///
    /// # Arguments
    ///
    /// - `query`: GraphQL+- query segment.
    /// - `first`:  number of items returned in one chunk
    ///
    /// # Errors
    ///
    /// * gRPC error
    /// * If transaction is not initialized properly, return `EmptyTxn` error.
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    /// use anyhow::Result;
    /// use serde::Deserialize;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// fn main() {
    ///     let query = r#"query stream($first: string, $offset: string) {
    ///         items(func: eq(name, "Alice"), first: $first, offset: $offset) {
    ///             uid
    ///             name
    ///         }
    ///     }"#;
    ///
    ///   let client = client();
    ///   let iterator = client.new_read_only_txn().into_iter(query,100);
    ///   let alices: Vec<Result<Person>> = iterator.collect();
    /// }
    /// ```
    ///
    pub fn into_iter<Q, T>(self, query: Q, first: usize) -> impl Iterator<Item = Result<T>>
    where
        Q: Into<String>,
        T: DeserializeOwned,
    {
        self.into_iter_with_vars(query, HashMap::<String, String>::new(), first)
    }

    ///
    /// Readonly transaction is transformed into iterator.
    ///
    /// Input `query` must accept **$first: string, $offset: string** arguments which are used for paginating.
    /// Iterator items must be returned in query block named **items**.
    ///
    /// # Return
    ///
    /// Iterator contains deserialized items returned from query.
    /// Iterato item is Ok(T) if **items** query data can be serialized into Vec<T>.
    ///
    /// # Arguments
    ///
    /// - `query`: GraphQL+- query segment.
    /// - `vars`: map of variables for query
    /// - `first`:  number of items returned in one chunk
    ///
    /// # Errors
    ///
    /// * gRPC error
    /// * If transaction is not initialized properly, return `EmptyTxn` error.
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    /// use anyhow::Result;
    /// use std::collections::HashMap;
    /// use serde::Deserialize;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// fn main() {
    ///     let query = r#"query stream($first: string, $offset: string, $name: string) {
    ///         items(func: eq(name, $name), first: $first, offset: $offset) {
    ///             uid
    ///             name
    ///         }
    ///     }"#;
    ///
    ///   let client = client();
    ///   let mut vars = HashMap::new();
    ///   vars.insert("$name", "Alice");
    ///   let iterator = client.new_read_only_txn().into_iter_with_vars(query, vars, 100);
    ///   let alices: Vec<Result<Person>> = iterator.collect();
    /// }
    /// ```
    ///
    pub fn into_iter_with_vars<Q, T, K, V>(
        self,
        query: Q,
        vars: HashMap<K, V>,
        first: usize,
    ) -> impl Iterator<Item = Result<T>>
    where
        Q: Into<String>,
        T: DeserializeOwned,
        K: Into<String> + Eq + Hash,
        V: Into<String>,
    {
        assert_ne!(
            first, 0,
            "First attribute for stream must not be eq to zero"
        );
        IteratorState::new(self, query, vars, first)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use serde_derive::{Deserialize, Serialize};

    use crate::sync::txn::mutated::Mutate;
    #[cfg(feature = "acl")]
    use crate::sync::AclClientType;
    use crate::sync::Client;
    #[cfg(feature = "acl")]
    use crate::LazyChannel;
    use crate::Mutation;

    #[cfg(not(feature = "acl"))]
    fn client() -> Client {
        Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    }

    #[cfg(feature = "acl")]
    fn client() -> AclClientType<LazyChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").expect("Acl client")
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    struct Car {
        uid: String,
        color: String,
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    struct Person {
        uid: String,
        name: String,
    }

    #[test]
    fn iterator() {
        let client = client();
        client.drop_all().expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .expect("Schema is not updated");
        let txn = client.new_mutated_txn();
        let data = vec![
            Car {
                uid: "_:a".to_string(),
                color: "A".to_string(),
            },
            Car {
                uid: "_:b".to_string(),
                color: "B".to_string(),
            },
            Car {
                uid: "_:c".to_string(),
                color: "C".to_string(),
            },
        ];
        let mut mu = Mutation::new();
        mu.set_set_json(&data).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
        let iterator = client.new_read_only_txn().into_iter(
            r#"
            query stream($first: string, $offset: string) {
                items(func: has(color), first: $first, offset: $offset) {{
                    uid
                    color
                }}
            }
        "#,
            2,
        );
        let cars: Vec<Result<Car>> = iterator.collect();
        assert_eq!(cars.len(), 3);
        assert!(cars.iter().all(|car| car.is_ok()))
    }

    #[test]
    fn iterator_with_vars() {
        let client = client();
        client.drop_all().expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .expect("Schema is not updated");
        let txn = client.new_mutated_txn();
        let data = vec![
            Car {
                uid: "_:a".to_string(),
                color: "A".to_string(),
            },
            Car {
                uid: "_:b".to_string(),
                color: "A".to_string(),
            },
            Car {
                uid: "_:c".to_string(),
                color: "C".to_string(),
            },
        ];
        let mut mu = Mutation::new();
        mu.set_set_json(&data).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
        let mut vars = HashMap::new();
        vars.insert("$color", "A");
        let iterator = client.new_read_only_txn().into_iter_with_vars(
            r#"
            query stream($first: string, $offset: string, $color: string) {
                items(func: eq(color, "A"), first: $first, offset: $offset) {{
                    uid
                    color
                }}
            }
        "#,
            vars,
            2,
        );
        let cars: Vec<Result<Car>> = iterator.collect();
        assert_eq!(cars.len(), 2);
        assert!(cars.iter().all(|car| car.is_ok()))
    }

    #[test]
    fn invalid_data_in_iterator() {
        let client = client();
        client.drop_all().expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .expect("Schema is not updated");
        let txn = client.new_mutated_txn();
        let data = vec![
            Car {
                uid: "_:a".to_string(),
                color: "A".to_string(),
            },
            Car {
                uid: "_:b".to_string(),
                color: "B".to_string(),
            },
            Car {
                uid: "_:c".to_string(),
                color: "C".to_string(),
            },
        ];
        let mut mu = Mutation::new();
        mu.set_set_json(&data).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
        let stream = client.new_read_only_txn().into_iter(
            r#"
            query stream($first: string, $offset: string) {
                items(func: has(color), first: $first, offset: $offset) {{
                    uid
                    color
                }}
            }
        "#,
            2,
        );
        let cars: Vec<Result<Person>> = stream.collect();
        assert_eq!(cars.len(), 1);
        assert!(cars.iter().all(|car| car.is_err()))
    }
}
