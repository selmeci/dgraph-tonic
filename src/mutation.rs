use std::ops::{Deref, DerefMut};

use serde::Serialize;
use serde_json::Error;

use crate::Mu;

pub struct Mutation(Mu);

impl Deref for Mutation {
    type Target = Mu;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Mutation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Mutation {
    pub(crate) fn mu(self) -> Mu {
        self.0
    }

    pub fn new() -> Self {
        Self(Mu::default())
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
