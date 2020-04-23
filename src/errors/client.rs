use crate::Status;
use failure::Fail;

///
/// Possible errors for client
///
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Client: invalid endpoint")]
    InvalidEndpoint,
    #[fail(display = "Client: no endpoints defined")]
    NoEndpointsDefined,
    #[fail(display = "Client: cannot do alter on DB")]
    CannotAlter(Status),
    #[fail(display = "Client: cannot login")]
    CannotLogin(Status),
    #[fail(display = "Client: cannot refresh login")]
    CannotRefreshLogin(Status),
    #[fail(display = "Client: cannot query")]
    CannotQuery(Status),
    #[fail(display = "Client: cannot mutate")]
    CannotMutate(Status),
    #[fail(display = "Client: cannot do request")]
    CannotDoRequest(Status),
    #[fail(display = "Client: cannot commit or abort")]
    CannotCommitOrAbort(Status),
    #[fail(display = "Client: cannot check version")]
    CannotCheckVersion(Status),
}
