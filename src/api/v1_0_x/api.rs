#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    #[prost(string, tag = "1")]
    pub query: ::prost::alloc::string::String,
    /// Support for GraphQL like variables.
    #[prost(map = "string, string", tag = "2")]
    pub vars:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(uint64, tag = "13")]
    pub start_ts: u64,
    #[prost(message, optional, tag = "14")]
    pub lin_read: ::core::option::Option<LinRead>,
    #[prost(bool, tag = "15")]
    pub read_only: bool,
    #[prost(bool, tag = "16")]
    pub best_effort: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    #[prost(bytes = "vec", tag = "1")]
    pub json: ::prost::alloc::vec::Vec<u8>,
    #[deprecated]
    #[prost(message, repeated, tag = "2")]
    pub schema: ::prost::alloc::vec::Vec<SchemaNode>,
    #[prost(message, optional, tag = "3")]
    pub txn: ::core::option::Option<TxnContext>,
    #[prost(message, optional, tag = "12")]
    pub latency: ::core::option::Option<Latency>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Assigned {
    #[prost(map = "string, string", tag = "1")]
    pub uids:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub context: ::core::option::Option<TxnContext>,
    #[prost(message, optional, tag = "12")]
    pub latency: ::core::option::Option<Latency>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Mutation {
    #[prost(bytes = "vec", tag = "1")]
    pub set_json: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub delete_json: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub set_nquads: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub del_nquads: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "5")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub cond: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "10")]
    pub set: ::prost::alloc::vec::Vec<NQuad>,
    #[prost(message, repeated, tag = "11")]
    pub del: ::prost::alloc::vec::Vec<NQuad>,
    #[prost(uint64, tag = "13")]
    pub start_ts: u64,
    #[prost(bool, tag = "14")]
    pub commit_now: bool,
    /// this field is not parsed and used by the server anymore.
    #[prost(bool, tag = "15")]
    pub ignore_index_conflict: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operation {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub drop_attr: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub drop_all: bool,
    #[prost(enumeration = "operation::DropOp", tag = "4")]
    pub drop_op: i32,
    /// If drop_op is ATTR or TYPE, drop_value holds the name of the predicate or
    /// type to delete.
    #[prost(string, tag = "5")]
    pub drop_value: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Operation`.
pub mod operation {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum DropOp {
        None = 0,
        All = 1,
        Data = 2,
        Attr = 3,
        Type = 4,
    }
}
/// Worker services.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxnContext {
    #[prost(uint64, tag = "1")]
    pub start_ts: u64,
    #[prost(uint64, tag = "2")]
    pub commit_ts: u64,
    #[prost(bool, tag = "3")]
    pub aborted: bool,
    /// List of keys to be used for conflict detection.
    #[prost(string, repeated, tag = "4")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// List of predicates involved in this transaction.
    #[prost(string, repeated, tag = "5")]
    pub preds: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "13")]
    pub lin_read: ::core::option::Option<LinRead>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Check {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Version {
    #[prost(string, tag = "1")]
    pub tag: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LinRead {
    #[prost(map = "uint32, uint64", tag = "1")]
    pub ids: ::std::collections::HashMap<u32, u64>,
    #[prost(enumeration = "lin_read::Sequencing", tag = "2")]
    pub sequencing: i32,
}
/// Nested message and enum types in `LinRead`.
pub mod lin_read {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Sequencing {
        ClientSide = 0,
        ServerSide = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Latency {
    #[prost(uint64, tag = "1")]
    pub parsing_ns: u64,
    #[prost(uint64, tag = "2")]
    pub processing_ns: u64,
    #[prost(uint64, tag = "3")]
    pub encoding_ns: u64,
    #[prost(uint64, tag = "4")]
    pub assign_timestamp_ns: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NQuad {
    #[prost(string, tag = "1")]
    pub subject: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub predicate: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub object_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub object_value: ::core::option::Option<Value>,
    #[prost(string, tag = "5")]
    pub label: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub lang: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "7")]
    pub facets: ::prost::alloc::vec::Vec<Facet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Val", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11")]
    pub val: ::core::option::Option<value::Val>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Val {
        #[prost(string, tag = "1")]
        DefaultVal(::prost::alloc::string::String),
        #[prost(bytes, tag = "2")]
        BytesVal(::prost::alloc::vec::Vec<u8>),
        #[prost(int64, tag = "3")]
        IntVal(i64),
        #[prost(bool, tag = "4")]
        BoolVal(bool),
        #[prost(string, tag = "5")]
        StrVal(::prost::alloc::string::String),
        #[prost(double, tag = "6")]
        DoubleVal(f64),
        /// Geo data in WKB format
        #[prost(bytes, tag = "7")]
        GeoVal(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "8")]
        DateVal(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "9")]
        DatetimeVal(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag = "10")]
        PasswordVal(::prost::alloc::string::String),
        #[prost(uint64, tag = "11")]
        UidVal(u64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Facet {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration = "facet::ValType", tag = "3")]
    pub val_type: i32,
    /// tokens of value.
    #[prost(string, repeated, tag = "4")]
    pub tokens: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// not stored, only used for query.
    #[prost(string, tag = "5")]
    pub alias: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Facet`.
pub mod facet {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ValType {
        String = 0,
        Int = 1,
        Float = 2,
        Bool = 3,
        Datetime = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaNode {
    #[prost(string, tag = "1")]
    pub predicate: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub index: bool,
    #[prost(string, repeated, tag = "4")]
    pub tokenizer: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "5")]
    pub reverse: bool,
    #[prost(bool, tag = "6")]
    pub count: bool,
    #[prost(bool, tag = "7")]
    pub list: bool,
    #[prost(bool, tag = "8")]
    pub upsert: bool,
    #[prost(bool, tag = "9")]
    pub lang: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoginRequest {
    #[prost(string, tag = "1")]
    pub userid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub refresh_token: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Jwt {
    #[prost(string, tag = "1")]
    pub access_jwt: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub refresh_jwt: ::prost::alloc::string::String,
}
#[doc = r" Generated client implementations."]
pub mod dgraph_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = " Graph response."]
    #[derive(Debug, Clone)]
    pub struct DgraphClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DgraphClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DgraphClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DgraphClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            DgraphClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn login(
            &mut self,
            request: impl tonic::IntoRequest<super::LoginRequest>,
        ) -> Result<tonic::Response<super::Response>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/Login");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn query(
            &mut self,
            request: impl tonic::IntoRequest<super::Request>,
        ) -> Result<tonic::Response<super::Response>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/Query");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn mutate(
            &mut self,
            request: impl tonic::IntoRequest<super::Mutation>,
        ) -> Result<tonic::Response<super::Assigned>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/Mutate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn alter(
            &mut self,
            request: impl tonic::IntoRequest<super::Operation>,
        ) -> Result<tonic::Response<super::Payload>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/Alter");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn commit_or_abort(
            &mut self,
            request: impl tonic::IntoRequest<super::TxnContext>,
        ) -> Result<tonic::Response<super::TxnContext>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/CommitOrAbort");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn check_version(
            &mut self,
            request: impl tonic::IntoRequest<super::Check>,
        ) -> Result<tonic::Response<super::Version>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/api.Dgraph/CheckVersion");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
