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
}
