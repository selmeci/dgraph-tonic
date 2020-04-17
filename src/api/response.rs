use serde::de::{self};
use serde_json::error::Error;
use serde_json::Value;

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

impl From<Response> for Value {
    fn from(reps: Response) -> Self {
        serde_json::from_slice(&reps.json).expect("JSON")
    }
}
