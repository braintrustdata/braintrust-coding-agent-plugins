use serde::Serialize;

#[derive(Serialize)]
pub(super) struct TrustState<'a> {
    pub(super) trusted_hash: &'a str,
}
