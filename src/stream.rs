use crate::client::ILazyClient;
use crate::{Query, TxnReadOnlyType};
use async_stream::try_stream;
use failure::Error;
use futures::stream::Stream;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Deserialize)]
struct Chunk<T> {
    items: Vec<T>,
}

impl<T: DeserializeOwned> Default for Chunk<T> {
    fn default() -> Self {
        Self {
            items: Vec::with_capacity(0),
        }
    }
}

impl<C: ILazyClient> TxnReadOnlyType<C> {
    async fn fetch_chunk<Q, T>(
        &mut self,
        query: Q,
        vars: HashMap<String, String>,
    ) -> Result<Vec<T>, Error>
    where
        Q: Into<String> + Send + Sync,
        T: DeserializeOwned,
    {
        let chunk: Chunk<T> = self.query_with_vars(query, vars).await?.try_into_owned()?;
        Ok(chunk.items)
    }

    ///
    /// Readonly transaction is transformed into async stream.
    ///
    /// Input `query` must accept **$first: string, $offset: string** arguments which are used for paginating.
    /// Stream items must be returned in query block named **items**.
    ///
    /// # Return
    ///
    /// Stream contains deserialized items returned from query.
    /// Stream item is Ok(T) if **items** query data can be serialized into Vec<T>.
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
    /// use std::collections::HashMap;
    /// use failure::Error;
    /// use futures::pin_mut;
    /// use futures::stream::StreamExt;
    /// use dgraph_tonic::Client;
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let query = r#"query stream($first: string, $offset: string) {
    ///         items(func: eq(name, "Alice"), first: $first, offset: $offset) {
    ///             uid
    ///             name
    ///         }
    ///     }"#;
    ///
    ///   let client = client().await;
    ///   let stream = client.new_read_only_txn().into_stream(query,100);
    ///   pin_mut!(stream);
    ///   let alices: Vec<Result<Person, Error>> = stream.collect().await;
    /// }
    /// ```
    ///
    pub fn into_stream<Q, T>(self, query: Q, first: usize) -> impl Stream<Item = Result<T, Error>>
    where
        Q: Into<String> + Send + Sync,
        T: Unpin + DeserializeOwned,
    {
        self.into_stream_with_vars(query, HashMap::<String, String>::new(), first)
    }

    ///
    /// Readonly transaction is transformed into async stream.
    ///
    /// Input `query` must accept **$first: string, $offset: string** arguments which are used for paginating.
    /// Stream items must be returned in query block named **items**.
    ///
    /// # Return
    ///
    /// Stream contains deserialized items returned from query.
    /// Stream item is Ok(T) if **items** query data can be serialized into Vec<T>.
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
    /// use std::collections::HashMap;
    /// use failure::Error;
    /// use futures::pin_mut;
    /// use futures::stream::StreamExt;
    /// use dgraph_tonic::{Client, Query};
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let query = r#"query stream($first: string, $offset: string, $name: string) {
    ///         items(func: eq(name, $name), first: $first, offset: $offset) {
    ///             uid
    ///             name
    ///         }
    ///     }"#;
    ///   
    ///   let mut vars = HashMap::new();
    ///   vars.insert("$name", "Alice");   
    ///   let client = client().await;
    ///   let stream = client.new_read_only_txn().into_stream_with_vars(query, vars, 100);
    ///   pin_mut!(stream);
    ///   let alices: Vec<Result<Person, Error>> = stream.collect().await;
    /// }
    /// ```
    ///
    pub fn into_stream_with_vars<Q, T, K, V>(
        mut self,
        query: Q,
        vars: HashMap<K, V>,
        first: usize,
    ) -> impl Stream<Item = Result<T, Error>>
    where
        Q: Into<String> + Send + Sync,
        T: Unpin + DeserializeOwned,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        assert_ne!(
            first, 0,
            "First attribute for stream must not be eq to zero"
        );
        let mut vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        vars.insert(String::from("$first"), format!("{}", first));
        let query = query.into();
        try_stream! {
            let mut offset = 0;
            loop {
                vars.insert(String::from("$offset"), format!("{}", offset));
                let chunk = self
                    .fetch_chunk(query.to_owned(), vars.to_owned())
                    .await?;
                if chunk.is_empty() {
                    break;
                };
                let chunk_len = chunk.len();
                for item in chunk {
                    offset += 1;
                    yield item
                }
                if chunk_len < first {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use serde_derive::{Deserialize, Serialize};

    use crate::client::Client;
    #[cfg(feature = "acl")]
    use crate::client::{AclClientType, LazyChannel};
    use crate::{Mutate, Mutation};
    use failure::Error;
    use futures::pin_mut;
    use futures::stream::StreamExt;
    use std::collections::HashMap;

    #[cfg(not(feature = "acl"))]
    async fn client() -> Client {
        Client::new("http://127.0.0.1:19080").unwrap()
    }

    #[cfg(feature = "acl")]
    async fn client() -> AclClientType<LazyChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").await.unwrap()
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

    #[tokio::test]
    async fn stream() {
        let client = client().await;
        client.drop_all().await.expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .await
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
        let response = txn.mutate_and_commit_now(mu).await;
        assert!(response.is_ok());
        let stream = client.new_read_only_txn().into_stream(
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
        pin_mut!(stream);
        let cars: Vec<Result<Car, Error>> = stream.collect().await;
        assert_eq!(cars.len(), 3);
        assert!(cars.iter().all(|car| car.is_ok()))
    }

    #[tokio::test]
    async fn stream_with_vars() {
        let client = client().await;
        client.drop_all().await.expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .await
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
        let response = txn.mutate_and_commit_now(mu).await;
        assert!(response.is_ok());
        let mut vars = HashMap::new();
        vars.insert("$color", "A");
        let stream = client.new_read_only_txn().into_stream_with_vars(
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
        pin_mut!(stream);
        let cars: Vec<Result<Car, Error>> = stream.collect().await;
        assert_eq!(cars.len(), 2);
        assert!(cars.iter().all(|car| car.is_ok()))
    }

    #[tokio::test]
    async fn invalid_data_in_stream() {
        let client = client().await;
        client.drop_all().await.expect("Data not dropped");
        client
            .set_schema("color: string @index(exact) .")
            .await
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
        let response = txn.mutate_and_commit_now(mu).await;
        assert!(response.is_ok());
        let stream = client.new_read_only_txn().into_stream(
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
        pin_mut!(stream);
        let cars: Vec<Result<Person, Error>> = stream.collect().await;
        assert_eq!(cars.len(), 1);
        assert!(cars.iter().all(|car| car.is_err()))
    }
}
