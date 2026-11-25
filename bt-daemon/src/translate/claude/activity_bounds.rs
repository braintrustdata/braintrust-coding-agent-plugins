//! Keep continuations, workflow calls, and nested agents open through their
//! descendants' work while preserving their native parent edges.
use crate::translate::{recent::RecentMap, SpanOp, SpanRow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize, Serialize)]
struct Activity {
    parent: Option<String>,
    end: Option<i64>,
    child_end: Option<i64>,
}

#[derive(Default, Deserialize, Serialize)]
pub(super) struct ActivityBounds {
    spans: RecentMap<String, Activity>,
}

impl ActivityBounds {
    pub(super) fn track(&mut self, id: &str) {
        if self.spans.get(id).is_none() {
            self.spans.insert(id.to_owned(), Activity::default());
        }
    }

    pub(super) fn extend_ancestors(&mut self, ops: &mut Vec<SpanOp>) {
        let mut extensions = Vec::new();
        for op in ops.iter_mut() {
            let row = op.row_mut();
            let Some(mut activity) = self.spans.get(&row.span_id).cloned() else {
                continue;
            };
            if let Some(parent) = row.parent_span_ids.first() {
                activity.parent = Some(parent.clone());
            }
            if let Some(end) = row.end_ms {
                let end = end
                    .max(activity.child_end.unwrap_or(end))
                    .max(activity.end.unwrap_or(end));
                activity.end = Some(end);
                row.end_ms = Some(end);
                self.extend_parents(
                    activity.parent.as_deref(),
                    end,
                    &row.root_span_id,
                    &mut extensions,
                );
            }
            self.spans.insert(row.span_id.clone(), activity);
        }
        // A later operation in the batch can already end the same span.
        ops.extend(extensions.into_iter().filter(|op| {
            let row = op.row();
            self.spans
                .get(&row.span_id)
                .is_some_and(|activity| activity.end == row.end_ms)
        }));
    }

    fn extend_parents(
        &mut self,
        parent: Option<&str>,
        end: i64,
        root: &str,
        extensions: &mut Vec<SpanOp>,
    ) {
        let mut parent = parent.map(str::to_owned);
        // Continue the walk only while the child's end increases the known
        // bound. This also terminates if malformed native records form a cycle.
        while let Some(id) = parent {
            let Some(mut activity) = self.spans.get(&id).cloned() else {
                break;
            };
            if activity.child_end.is_some_and(|previous| previous >= end) {
                break;
            }
            activity.child_end = Some(end);
            if activity.end.is_some_and(|previous| previous < end) {
                activity.end = Some(end);
                extensions.push(SpanOp::Merge(SpanRow {
                    span_id: id.clone(),
                    root_span_id: root.to_owned(),
                    parent_span_ids: activity.parent.iter().cloned().collect(),
                    end_ms: Some(end),
                    late_merge_key: Some(format!("activity:child:{end}")),
                    ..Default::default()
                }));
            }
            parent = activity.parent.clone();
            self.spans.insert(id, activity);
        }
    }
}
