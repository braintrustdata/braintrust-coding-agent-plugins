//! Native correlation identifiers are separate domains even though their wire
//! representation is a string. Composite span keys are constructed by reducers.
use serde::{Deserialize, Serialize};
macro_rules! identifier {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub(crate) struct $name(String);
        impl From<String> for $name { fn from(value: String) -> Self { Self(value) } }
        impl From<&str> for $name { fn from(value: &str) -> Self { Self(value.into()) } }
        impl std::ops::Deref for $name { type Target = str; fn deref(&self) -> &str { &self.0 } }
        impl std::fmt::Display for $name { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) } }
    )+};
}
identifier!(TurnId, RequestId, ToolCallId);
