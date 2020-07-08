use crate::Status;

///
/// Possible errors for client
///
#[derive(Debug)]
pub enum Error {
    InvalidEndpoint,
    NoEndpointsDefined,
    CannotAlter(Status),
    CannotLogin(Status),
    CannotRefreshLogin(Status),
    CannotQuery(Status),
    CannotMutate(Status),
    CannotDoRequest(Status),
    CannotCommitOrAbort(Status),
    CannotCheckVersion(Status),
    Transport(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Error::InvalidEndpoint => write!(f, "Client: invalid endpoint"),
            Error::NoEndpointsDefined => write!(f, "Client: no endpoints defined"),
            Error::CannotAlter(_) => write!(f, "Client: cannot do alter on DB"),
            Error::CannotLogin(_) => write!(f, "Client: cannot login"),
            Error::CannotRefreshLogin(_) => write!(f, "Client: cannot refresh login"),
            Error::CannotQuery(_) => write!(f, "Client: cannot query"),
            Error::CannotMutate(_) => write!(f, "Client: cannot mutate"),
            Error::CannotDoRequest(_) => write!(f, "Client: cannot do request"),
            Error::CannotCommitOrAbort(_) => write!(f, "Client: cannot commit or abort"),
            Error::CannotCheckVersion(_) => write!(f, "Client: cannot check version"),
            Error::Transport(e) => write!(f, "Transport error: {}", e),
        }
    }
}

impl std::error::Error for Error {}
