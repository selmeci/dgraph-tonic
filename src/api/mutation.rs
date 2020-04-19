use serde::Serialize;
use serde_json::Error;

use crate::Mutation;

impl Mutation {
    ///
    /// Create new dGraph Mutation object.
    ///
    /// Mutation represent required modification of data in DB.
    /// Mutation provides two main ways to set data: JSON and RDF N-Quad.
    /// You can choose whichever way is convenient.
    /// JSON way has implemented to helper functions.
    ///
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    ///
    /// Can be applied on a Mutation object to not run conflict detection over the index,
    /// which would decrease the number of transaction conflicts and aborts.
    /// However, this would come at the cost of potentially inconsistent upsert operations.
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::Mutation;
    /// let mut mu = Mutation::new().with_ignored_index_conflict();
    /// ```
    ///
    pub fn with_ignored_index_conflict(mut self) -> Self {
        self.ignore_index_conflict = true;
        self
    }

    ///
    /// Set set JSON data in Mutation.
    ///
    /// # Arguments
    ///
    /// * `value` - ref to struct which can be serialized into JSON
    ///
    /// # Errors
    ///
    /// Return serde_json:Error when value cannot be serialized to JSON format
    ///
    /// # Examples
    ///
    /// ```
    /// #[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// let p = Person {
    ///   uid:  "_:alice".into(),
    ///   name: "Alice".into(),
    /// };
    ///
    /// let mut mu = Mutation::new();
    /// mu.set_set_json(&p)?;
    /// ```
    ///
    pub fn set_set_json<T: ?Sized>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let set_json = serde_json::to_vec(&value)?;
        self.set_json = set_json;
        Ok(())
    }

    ///
    /// Set delete JSON data in Mutation.
    ///
    /// # Arguments
    ///
    /// * `value` - ref to struct which can be serialized into JSON
    ///
    /// # Errors
    ///
    /// Return serde_json:Error when value cannot be serialized to JSON format
    ///
    /// # Examples
    ///
    /// ```
    /// #[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: Option<String>,
    /// }
    ///
    /// let p = Person {
    ///   uid:  "_:0x1".into(),
    ///   name: None,
    /// };
    ///
    /// let mut mu = Mutation::new();
    /// //remove name predicate
    /// mu.set_delete_json(&p)?;
    /// ```
    ///
    pub fn set_delete_json<T: ?Sized>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let delete_json = serde_json::to_vec(&value)?;
        self.delete_json = delete_json;
        Ok(())
    }
}
