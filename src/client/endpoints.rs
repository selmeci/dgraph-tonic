use http::Uri;
use std::convert::TryInto;

///
/// Helper struct for endpoints input argument in new client function.
/// Allows to create client with one or more endpoints.
///
#[derive(Debug)]
pub struct Endpoints<S: TryInto<Uri>> {
    pub(crate) endpoints: Vec<S>,
}

impl<S: TryInto<Uri>> From<Vec<S>> for Endpoints<S> {
    fn from(endpoints: Vec<S>) -> Self {
        Self { endpoints }
    }
}

impl<S: TryInto<Uri>> From<S> for Endpoints<S> {
    fn from(endpoint: S) -> Self {
        Self {
            endpoints: vec![endpoint],
        }
    }
}
