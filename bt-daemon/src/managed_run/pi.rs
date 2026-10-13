//! Pi managed runs load the tracing extension with `-e`.

use std::ffi::OsString;

use super::{Injection, ManagedRun};
use crate::agents::Pi;
use crate::args::RunHookCommand;

impl ManagedRun for Pi {
    fn executable(&self) -> (&'static str, &'static str) {
        ("PI_BIN", "pi")
    }

    fn inject(
        &self,
        _hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        let extension = match crate::env::var_os("BRAINTRUST_TRACE_PI_PLUGIN_SPEC") {
            Some(extension) => extension,
            None => OsString::from(crate::setup::pi::plugin_spec()),
        };
        Ok(Injection {
            args: vec![OsString::from("-e"), extension],
            ..Injection::default()
        })
    }
}
