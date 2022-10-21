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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_vector() {
        let urls = vec!["http://localhost:2379", "http://localhost:22379"];
        let endpoints = Endpoints::from(urls.clone());
        assert_eq!(endpoints.endpoints.len(), 2);
        assert_eq!(endpoints.endpoints[0], urls[0]);
        assert_eq!(endpoints.endpoints[1], urls[1]);
    }

    #[test]
    fn from_str() {
        let url = "http://localhost:2379";
        let endpoints = Endpoints::from(url);
        assert_eq!(endpoints.endpoints.len(), 1);
        assert_eq!(endpoints.endpoints[0], url);
    }
}
