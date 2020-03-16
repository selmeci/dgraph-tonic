use serde::de::{self};
use serde_json::error::Error;

use crate::Response;

impl Response {
    pub fn try_into<'a, T>(&'a self) -> Result<T, Error>
    where
        T: de::Deserialize<'a>,
    {
        let result: T = serde_json::from_slice(&self.json)?;
        Ok(result)
    }

    pub fn try_into_owned<T>(self) -> Result<T, Error>
    where
        T: de::DeserializeOwned,
    {
        let result: T = serde_json::from_slice(&self.json)?;
        Ok(result)
    }
}
