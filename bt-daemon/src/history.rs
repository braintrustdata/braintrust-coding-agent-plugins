//! History writing format shared by journals and future span reporting.
//! Callers choose snapshot versus delta and supply captured input and output
//! separately. Matching, stream identity, and delivery policy are not defined here.
use serde::Serialize;
use serde_json::{value::RawValue, Value};

/// Write a snapshot, or an append starting at a caller-selected boundary,
/// together with separately captured output. The caller owns prefix matching.
pub(crate) fn write(
    input: &Value,
    output: Option<&Value>,
    append_from: Option<usize>,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Serialize)]
    struct Record<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        input: Option<&'a Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        input_delta: Option<&'a [Value]>,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<&'a Value>,
    }
    let (input, input_delta) = match append_from {
        None => (Some(input), None),
        Some(start) => {
            let tail = input
                .as_array()
                .and_then(|items| items.get(start..))
                .ok_or_else(|| anyhow::anyhow!("invalid input delta boundary"))?;
            (None, Some(tail))
        }
    };
    Ok(serde_json::value::to_raw_value(&Record {
        input,
        input_delta,
        output,
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn writes_snapshots_and_deltas_with_separate_current_output() {
        let input = json!([{"role":"user","content":"hello","native":[true,1,null]}]);
        let output = json!([{"role":"assistant","content":"hi"}]);
        for (record, field) in [(None, "input"), (Some(0), "input_delta")] {
            let stored: Value =
                serde_json::from_str(write(&input, Some(&output), record).unwrap().get()).unwrap();
            assert_eq!(stored[field], json!(input));
            assert_eq!(stored["output"], json!(output));
            assert_eq!(stored.as_object().unwrap().len(), 2);
        }
        let missing: Value =
            serde_json::from_str(write(&input, None, None).unwrap().get()).unwrap();
        assert!(missing.get("output").is_none());
        for output in [json!(null), json!({"provider_response":{"text":"hi"}})] {
            let scalar = json!("full input");
            let stored: Value =
                serde_json::from_str(write(&scalar, Some(&output), None).unwrap().get()).unwrap();
            assert_eq!(stored, json!({"input":scalar,"output":output}));
        }
        let empty: Value =
            serde_json::from_str(write(&input, Some(&json!([])), Some(1)).unwrap().get()).unwrap();
        assert_eq!(empty, json!({"input_delta":[],"output":[]}));
    }
}
