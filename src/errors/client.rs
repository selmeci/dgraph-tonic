use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Client: invalid endpoint")]
    InvalidEndpoint,
    #[fail(display = "Client: no endpoints defined")]
    NoEndpointsDefined,
}
