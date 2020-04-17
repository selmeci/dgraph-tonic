use serde::Serialize;
use serde_json::Error;

use crate::Mutation;

impl Mutation {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn with_set_json<T: ?Sized>(mut self, value: &T) -> Result<Self, Error>
    where
        T: Serialize,
    {
        let set_json = serde_json::to_vec(&value)?;
        self.set_json = set_json;
        Ok(self)
    }

    pub fn with_delete_json<T: ?Sized>(mut self, value: &T) -> Result<Self, Error>
    where
        T: Serialize,
    {
        let delete_json = serde_json::to_vec(&value)?;
        self.delete_json = delete_json;
        Ok(self)
    }
}
