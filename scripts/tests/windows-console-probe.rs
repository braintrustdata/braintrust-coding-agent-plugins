// Standalone Windows fixture; compiled by windows-background-process.mjs with rustc.
use std::env;
use std::ffi::c_void;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::windows::process::CommandExt;
use std::path::Path;
use std::process::{self, Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DETACHED_PROCESS: u32 = 0x00000008;

#[link(name = "kernel32")]
extern "system" {
    fn GetConsoleWindow() -> *mut c_void;
    fn FreeConsole() -> i32;
    fn AttachConsole(pid: u32) -> i32;
    fn GetLastError() -> u32;
    fn CreateJobObjectW(attributes: *mut c_void, name: *const u16) -> *mut c_void;
    fn SetInformationJobObject(job: *mut c_void, class: i32, info: *const c_void, size: u32)
        -> i32;
    fn AssignProcessToJobObject(job: *mut c_void, process: *mut c_void) -> i32;
    fn GetCurrentProcess() -> *mut c_void;
}

fn has_console() -> bool {
    !unsafe { GetConsoleWindow() }.is_null()
}

#[repr(C)]
#[derive(Default)]
struct BasicLimitInformation {
    process_time: i64,
    job_time: i64,
    flags: u32,
    minimum_working_set: usize,
    maximum_working_set: usize,
    active_processes: u32,
    affinity: usize,
    priority: u32,
    scheduling: u32,
}

#[repr(C)]
#[derive(Default)]
struct ExtendedLimitInformation {
    basic: BasicLimitInformation,
    io_counters: [u64; 6],
    memory_limits: [usize; 4],
}

fn contain_descendants() {
    // Keep the non-inheritable handle alive until this parent exits. Windows
    // closes it even on panic/TerminateProcess, killing every remaining child.
    // This also works inside the nested job used by modern Windows CI runners.
    let job = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
    assert!(
        !job.is_null(),
        "CreateJobObjectW: {}",
        io::Error::last_os_error()
    );
    let mut limits = ExtendedLimitInformation::default();
    limits.basic.flags = 0x00002000; // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    assert_ne!(
        unsafe {
            SetInformationJobObject(
                job,
                9,
                &limits as *const _ as *const c_void,
                std::mem::size_of_val(&limits) as u32,
            )
        },
        0,
        "SetInformationJobObject: {}",
        io::Error::last_os_error()
    );
    assert_ne!(
        unsafe { AssignProcessToJobObject(job, GetCurrentProcess()) },
        0,
        "AssignProcessToJobObject: {}",
        io::Error::last_os_error()
    );
}

fn wait(mut child: Child, seconds: u64) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            return status.code().unwrap_or(1);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("console fixture child exceeded {seconds}s deadline");
            return 124;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--launch") => {
            // Redirect to files: the detached processes cannot inherit a console,
            // and the test still gets actionable stdout/stderr on failure.
            let dir = Path::new(&args[1]);
            let stdout = File::create(dir.join("exercise.stdout")).unwrap();
            let stderr = File::create(dir.join("exercise.stderr")).unwrap();
            let child = Command::new(env::current_exe().unwrap())
                .arg("--detached-parent")
                .args(&args[1..])
                .creation_flags(DETACHED_PROCESS)
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr)
                .spawn()
                .expect("launch detached native parent");
            process::exit(wait(child, 35));
        }
        Some("--detached-parent") => {
            assert!(!has_console(), "native test parent inherited a console");
            contain_descendants();
            fs::write(
                Path::new(&args[1]).join("native-parent.json"),
                "{\"console_window\":false}",
            )
            .unwrap();
            let child = Command::new(&args[2])
                .args(&args[3..])
                .creation_flags(DETACHED_PROCESS)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("launch detached JavaScript exercise");
            process::exit(wait(child, 30));
        }
        Some("--check-parent") => {
            // A console-free child alone cannot prove its parent was console-free.
            // AttachConsole must fail with ERROR_INVALID_HANDLE for the live Node PID.
            unsafe { FreeConsole() };
            let attached = unsafe { AttachConsole(args[1].parse().unwrap()) } != 0;
            let error = if attached {
                0
            } else {
                unsafe { GetLastError() }
            };
            if attached {
                unsafe { FreeConsole() };
            }
            println!("{{\"attached\":{attached},\"error\":{error}}}");
        }
        _ => {
            // A broken consumer must not leave a hanging native child in CI.
            thread::spawn(|| {
                thread::sleep(Duration::from_secs(20));
                process::exit(124);
            });
            let console = has_console();
            let fail = env::var("BT_CONSOLE_PROBE_MODE").as_deref() == Ok("fail");
            let code = if fail { 23 } else { 0 };
            if let Ok(report) = env::var("BT_CONSOLE_PROBE_REPORT") {
                fs::write(
                    report,
                    format!("{{\"console_window\":{console},\"exit_code\":{code}}}"),
                )
                .unwrap();
            }
            if args.first().map(String::as_str) == Some("--daemon") {
                // DaemonClient intentionally ignores all three standard handles.
                process::exit(code);
            }
            if args.first().map(String::as_str) == Some("--echo") {
                let mut input = String::new();
                io::stdin().read_to_string(&mut input).unwrap();
                print!("{input}");
                eprintln!("probe stderr: café 雪");
            } else if fail {
                eprintln!("intentional probe failure: café 雪");
            } else {
                assert_eq!(&args[..2], ["projects", "list"]);
                assert!(args.iter().any(|arg| arg == "--json"));
                assert!(args.iter().any(|arg| arg == "--no-input"));
                println!(
                    "[{{\"id\":\"probe\",\"name\":\"café 雪\",\"console_window\":{console}}}]"
                );
            }
            io::stdout().flush().unwrap();
            io::stderr().flush().unwrap();
            process::exit(code);
        }
    }
}
