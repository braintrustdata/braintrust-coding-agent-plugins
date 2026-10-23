//! Optional wire fields whose explicit null differs from absence in authored
//! metadata and replay fingerprints. Ordinary optional data uses Option instead.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[derive(Debug, Clone, Default)]
pub(crate) enum Field<T> {
    #[default]
    Missing,
    Null,
    Value(T),
}
impl<T> Field<T> {
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
    pub fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Value(v) => Some(v),
            _ => None,
        }
    }
}
impl<T: std::ops::Deref> Field<T> {
    pub fn as_deref(&self) -> Option<&T::Target> {
        self.as_ref().map(std::ops::Deref::deref)
    }
}
impl<T> From<Option<T>> for Field<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => Self::Value(v),
            None => Self::Null,
        }
    }
}
impl<T: Serialize> Serialize for Field<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Value(v) => v.serialize(s),
            Self::Null | Self::Missing => s.serialize_none(),
        }
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Field<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Option::<T>::deserialize(d)?.into())
    }
}
