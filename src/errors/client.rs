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
    #[error("Client: cannot do alter on DB.\n{0:?}")]
    CannotAlter(Status),
    #[error("Client: cannot login.\n{0:?}")]
    CannotLogin(Status),
    #[error("Client: cannot refresh login.\n{0:?}")]
    CannotRefreshLogin(Status),
    #[error("Client: cannot query.\n{0:?}")]
    CannotQuery(Status),
    #[error("Client: cannot mutate.\n{0:?}")]
    CannotMutate(Status),
    #[error("Client: cannot do request.\n{0:?}")]
    CannotDoRequest(Status),
    #[error("Client: cannot commit or abort.\n{0:?}")]
    CannotCommitOrAbort(Status),
    #[error("Client: cannot check version.\n{0:?}")]
    CannotCheckVersion(Status),
}
