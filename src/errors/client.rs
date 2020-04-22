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
}
