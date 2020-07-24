use anyhow::Result;

use crate::{DgraphError, TxnContext};

impl TxnContext {
    pub(crate) fn merge_context(&mut self, src: &TxnContext) -> Result<()> {
        if self.start_ts == 0 {
            self.start_ts = src.start_ts;
        } else if self.start_ts != src.start_ts {
            anyhow::bail!(DgraphError::StartTsMismatch)
        };
        let dedup = |data: &mut Vec<String>| {
            data.sort_unstable();
            data.dedup();
        };
        self.keys.append(&mut src.keys.clone());
        dedup(&mut self.keys);
        self.preds.append(&mut src.preds.clone());
        dedup(&mut self.preds);

        Ok(())
    }
}
