//! Launch the persistent daemon without inheriting the hook's output pipes.
//!
//! `std::process::Command` sets `bInheritHandles` on Windows when it redirects
//! standard streams. That also passes *other* inheritable handles in the hook
//! process to the daemon. Codex waits for EOF on the hook's stdout and stderr,
//! so an inherited write end keeps every hook blocked until the daemon exits.
//! An explicit process handle list confines inheritance to the three standard
//! handles we give the daemon.

use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io;
use std::mem::{size_of, size_of_val};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::ptr;
use windows_sys::Win32::Foundation::{DuplicateHandle, DUPLICATE_SAME_ACCESS, HANDLE};
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess,
    InitializeProcThreadAttributeList, UpdateProcThreadAttribute, CREATE_NEW_PROCESS_GROUP,
    DETACHED_PROCESS, EXTENDED_STARTUPINFO_PRESENT, PROCESS_INFORMATION,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, STARTF_USESTDHANDLES, STARTUPINFOEXW,
};

pub(crate) fn spawn_detached_daemon(
    program: &OsStr,
    args: &[OsString],
    log: Option<File>,
) -> io::Result<()> {
    let input = File::open("NUL")?;
    let output = match log {
        Some(log) => log,
        None => OpenOptions::new().write(true).open("NUL")?,
    };
    let input = inheritable_copy(&input)?;
    let stdout = inheritable_copy(&output)?;
    let stderr = inheritable_copy(&output)?;
    let handles = [
        input.as_raw_handle() as HANDLE,
        stdout.as_raw_handle() as HANDLE,
        stderr.as_raw_handle() as HANDLE,
    ];

    let attributes = HandleList::new(&handles)?;
    let application = wide_nul(program)?;
    let mut command_line = command_line(program, args)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = handles[0];
    startup.StartupInfo.hStdOutput = handles[1];
    startup.StartupInfo.hStdError = handles[2];
    startup.lpAttributeList = attributes.as_ptr();
    let mut process = PROCESS_INFORMATION::default();

    // SAFETY: All pointers refer to live, writable buffers. The allowlisted
    // handles remain valid through CreateProcessW, and the child inherits no
    // other handles from the hook process.
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            ptr::null(),
            ptr::null(),
            1,
            DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | EXTENDED_STARTUPINFO_PRESENT,
            ptr::null(),
            ptr::null(),
            &startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: A successful CreateProcessW returns handles owned by this caller.
    let _process = unsafe { OwnedHandle::from_raw_handle(process.hProcess) };
    let _thread = unsafe { OwnedHandle::from_raw_handle(process.hThread) };
    Ok(())
}

fn inheritable_copy(file: &File) -> io::Result<OwnedHandle> {
    let mut copy = ptr::null_mut();
    // SAFETY: The source file handle is valid and the current process owns it.
    let copied = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            file.as_raw_handle() as HANDLE,
            GetCurrentProcess(),
            &mut copy,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if copied == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: DuplicateHandle returned a new handle owned by this process.
    Ok(unsafe { OwnedHandle::from_raw_handle(copy) })
}

struct HandleList {
    storage: Vec<usize>,
    initialized: bool,
}

impl HandleList {
    fn new(handles: &[HANDLE; 3]) -> io::Result<Self> {
        let mut bytes = 0;
        // The first call obtains the required buffer size. It normally fails
        // with ERROR_INSUFFICIENT_BUFFER while setting `bytes`.
        unsafe { InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut bytes) };
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        // Attribute lists need pointer alignment; Vec<u8> does not promise it.
        let storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        let mut list = Self {
            storage,
            initialized: false,
        };
        // SAFETY: The allocated aligned buffer has the size requested above.
        if unsafe { InitializeProcThreadAttributeList(list.as_ptr(), 1, 0, &mut bytes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        list.initialized = true;
        // SAFETY: The handle array stays alive until process creation returns.
        if unsafe {
            UpdateProcThreadAttribute(
                list.as_ptr(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast(),
                size_of_val(handles),
                ptr::null_mut(),
                ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(list)
    }

    fn as_ptr(&self) -> *mut core::ffi::c_void {
        self.storage.as_ptr().cast_mut().cast()
    }
}

impl Drop for HandleList {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: Initialization succeeded before this flag was set.
            unsafe { DeleteProcThreadAttributeList(self.as_ptr()) };
        }
    }
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide: Vec<u16> = value.encode_wide().collect();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in executable path",
        ));
    }
    wide.push(0);
    Ok(wide)
}

fn command_line(program: &OsStr, args: &[OsString]) -> io::Result<Vec<u16>> {
    let mut result = Vec::new();
    for arg in std::iter::once(program).chain(args.iter().map(OsString::as_os_str)) {
        if !result.is_empty() {
            result.push(b' ' as u16);
        }
        quote_argument(arg, &mut result)?;
    }
    result.push(0);
    Ok(result)
}

// Quote for the Windows C runtime argv parser, including backslashes before
// quotes and the closing quote. No shell is involved.
fn quote_argument(arg: &OsStr, output: &mut Vec<u16>) -> io::Result<()> {
    const BACKSLASH: u16 = b'\\' as u16;
    const QUOTE: u16 = b'"' as u16;
    output.push(b'"' as u16);
    let mut backslashes = 0;
    for unit in arg.encode_wide() {
        match unit {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NUL in argument",
                ))
            }
            BACKSLASH => backslashes += 1,
            QUOTE => {
                output.extend(std::iter::repeat_n(BACKSLASH, backslashes * 2 + 1));
                output.push(unit);
                backslashes = 0;
            }
            _ => {
                output.extend(std::iter::repeat_n(BACKSLASH, backslashes));
                output.push(unit);
                backslashes = 0;
            }
        }
    }
    output.extend(std::iter::repeat_n(BACKSLASH, backslashes * 2));
    output.push(b'"' as u16);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::spawn_detached_daemon;
    use std::ffi::OsString;
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn daemon_does_not_hold_hook_output_pipes_open() {
        const ROLE: &str = "_BT_DAEMON_HANDLE_TEST_ROLE";
        const DIR: &str = "_BT_DAEMON_HANDLE_TEST_DIR";
        const TEST: &str =
            "subprocess::windows_daemon::tests::daemon_does_not_hold_hook_output_pipes_open";

        match std::env::var(ROLE).as_deref() {
            Ok("hook")
                if std::path::Path::new(&std::env::var_os(DIR).unwrap())
                    .join("daemon.marker")
                    .exists() =>
            {
                let dir = std::env::var_os(DIR).unwrap();
                let dir = std::path::Path::new(&dir);
                fs::write(dir.join("ready"), b"").unwrap();
                let deadline = Instant::now() + Duration::from_secs(15);
                while !dir.join("stop").exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                fs::write(dir.join("done"), b"").unwrap();
            }
            Ok("hook") => {
                let dir = std::env::var_os(DIR).unwrap();
                let dir = std::path::Path::new(&dir);
                let log = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("serve.log"))
                    .unwrap();
                // The re-executed test process inherits ROLE. A file marker
                // distinguishes it without mutating process-wide environment
                // variables while other tests may be running.
                fs::write(dir.join("daemon.marker"), b"").unwrap();
                spawn_detached_daemon(
                    std::env::current_exe().unwrap().as_os_str(),
                    &[
                        OsString::from("--exact"),
                        OsString::from(TEST),
                        OsString::from("--nocapture"),
                    ],
                    Some(log),
                )
                .unwrap();
            }
            _ => {
                let dir = tempfile::tempdir().unwrap();
                let mut hook = Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(ROLE, "hook")
                    .env(DIR, dir.path())
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(5);
                while hook.try_wait().unwrap().is_none() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                let status = hook.try_wait().unwrap().unwrap_or_else(|| {
                    hook.kill().unwrap();
                    panic!("hook process did not exit");
                });
                assert!(status.success(), "hook process failed: {status}");
                while !dir.path().join("ready").exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                assert!(dir.path().join("ready").exists(), "daemon did not start");

                let (tx, rx) = mpsc::channel();
                fn read_pipe<R: Read + Send + 'static>(
                    mut pipe: R,
                    tx: mpsc::Sender<std::io::Result<usize>>,
                ) {
                    std::thread::spawn(move || {
                        let mut output = Vec::new();
                        tx.send(pipe.read_to_end(&mut output)).unwrap();
                    });
                }
                read_pipe(hook.stdout.take().unwrap(), tx.clone());
                read_pipe(hook.stderr.take().unwrap(), tx);
                let stdout_eof = rx.recv_timeout(Duration::from_secs(2)).is_ok();
                let stderr_eof = rx.recv_timeout(Duration::from_secs(2)).is_ok();
                // Always release the daemon, including when the old launch
                // behavior causes this assertion to fail.
                fs::write(dir.path().join("stop"), b"").unwrap();
                let deadline = Instant::now() + Duration::from_secs(5);
                while !dir.path().join("done").exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                assert!(dir.path().join("done").exists(), "daemon did not stop");
                assert!(
                    stdout_eof && stderr_eof,
                    "daemon kept hook stdout/stderr open"
                );
            }
        }
    }
}
