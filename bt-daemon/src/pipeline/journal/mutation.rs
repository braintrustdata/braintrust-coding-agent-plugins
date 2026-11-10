//! Typed collection mutations staged before a session-local storage commit.
use super::StoredOp;
use crate::translate::SpanOp;
use std::{collections::BTreeMap, ops::RangeToInclusive};

#[derive(Default)]
pub(crate) struct Mutations {
    pub events: Events,
    pub checkpoints: Checkpoints,
    pub roots: Roots,
}
#[derive(Default)]
pub(crate) struct Events {
    pub(super) through: Option<u64>,
}
impl Events {
    pub fn drain(&mut self, range: RangeToInclusive<u64>) -> anyhow::Result<()> {
        anyhow::ensure!(self.through.is_none(), "event drain already staged");
        self.through = Some(range.end);
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct Checkpoints {
    pub(super) value: Option<(u64, Vec<u8>)>,
}
impl Checkpoints {
    pub fn insert(&mut self, id: u64, value: Vec<u8>) -> anyhow::Result<()> {
        anyhow::ensure!(self.value.is_none(), "checkpoint already staged");
        self.value = Some((id, value));
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct Roots {
    pub(super) values: Option<Vec<(String, Vec<u8>)>>,
}
impl Roots {
    pub fn replace(&mut self, roots: &BTreeMap<String, Vec<SpanOp>>) -> anyhow::Result<()> {
        self.values = Some(
            roots
                .iter()
                .map(|(id, ops)| {
                    let ops = ops
                        .iter()
                        .map(StoredOp::encode)
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Ok((id.clone(), serde_json::to_vec(&ops)?))
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        );
        Ok(())
    }
}
