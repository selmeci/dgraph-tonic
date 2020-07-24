use thiserror::Error as Fail;

use crate::Status;

///
/// Possible errors for client
///
#[derive(Debug, Fail)]
pub enum Error {
    #[error("Client: invalid endpoint")]
    InvalidEndpoint,
    #[error("Client: no endpoints defined")]
    NoEndpointsDefined,
    #[error("Client: cannot do alter on DB")]
    CannotAlter(Status),
    #[error("Client: cannot login")]
    CannotLogin(Status),
    #[error("Client: cannot refresh login")]
    CannotRefreshLogin(Status),
    #[error("Client: cannot query")]
    CannotQuery(Status),
    #[error("Client: cannot mutate")]
    CannotMutate(Status),
    #[error("Client: cannot do request")]
    CannotDoRequest(Status),
    #[error("Client: cannot commit or abort")]
    CannotCommitOrAbort(Status),
    #[error("Client: cannot check version")]
    CannotCheckVersion(Status),
}
