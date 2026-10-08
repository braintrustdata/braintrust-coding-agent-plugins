//! Owner-only Windows security descriptors.
//!
//! Unix keeps daemon state private with `0600` files and `0700` directories.
//! Windows has no mode bits, and a directory's ACL alone does not protect the
//! files beneath it: Everyone holds the bypass-traverse privilege by default.
//! Every private object therefore carries its own protected DACL granting
//! only the current user and SYSTEM, the Windows counterpart of root.

use std::ffi::{c_void, OsStr};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::ptr::null_mut;

use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, ERROR_SUCCESS, HANDLE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
    GetNamedSecurityInfoW, SetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetTokenInformation,
    TokenUser, ACE_HEADER, ACL, DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// A security descriptor allocated by the system with `LocalAlloc`.
pub(crate) struct SecurityDescriptor(PSECURITY_DESCRIPTOR);

// The descriptor is immutable after construction and owned exclusively.
unsafe impl Send for SecurityDescriptor {}
unsafe impl Sync for SecurityDescriptor {}

impl Drop for SecurityDescriptor {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

impl SecurityDescriptor {
    pub(crate) fn from_sddl(sddl: &str) -> io::Result<Self> {
        let wide = wide(OsStr::new(sddl));
        let mut descriptor = null_mut();
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(descriptor))
    }

    /// Attributes for creating a kernel object with this descriptor. The
    /// result borrows `self`, which must outlive the creating call.
    pub(crate) fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }

    /// The descriptor's DACL, which lives inside the descriptor's allocation.
    fn dacl(&self) -> io::Result<&ACL> {
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl: *mut ACL = null_mut();
        let ok =
            unsafe { GetSecurityDescriptorDacl(self.0, &mut present, &mut dacl, &mut defaulted) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a non-null DACL points into `self`, which outlives the borrow.
        let dacl = if present != 0 {
            unsafe { dacl.as_ref() }
        } else {
            None
        };
        dacl.ok_or_else(|| io::Error::other("security descriptor has no DACL"))
    }
}

/// A protected DACL granting full access only to the current user and SYSTEM.
/// `inheritable` propagates it to files and directories created beneath.
pub(crate) fn owner_only(inheritable: bool) -> io::Result<SecurityDescriptor> {
    let user = current_user_sid()?;
    let flags = if inheritable { "OICI" } else { "" };
    SecurityDescriptor::from_sddl(&format!("D:P(A;{flags};FA;;;{user})(A;{flags};FA;;;SY)"))
}

/// Replace a file's or directory's DACL with an owner-only one. Setting a
/// directory's DACL re-applies inheritance to its existing children.
pub(crate) fn restrict_to_owner(path: &Path, directory: bool) -> io::Result<()> {
    let descriptor = owner_only(directory)?;
    set_dacl(path, &descriptor)
}

/// Whether `path` already carries exactly the DACL [`restrict_to_owner`]
/// installs, letting callers skip re-propagating a directory tree.
pub(crate) fn is_owner_only(path: &Path, directory: bool) -> io::Result<bool> {
    let expected = owner_only(directory)?;
    let current = NamedDacl::read(path)?;
    let mut control = 0;
    let mut revision = 0;
    if unsafe { GetSecurityDescriptorControl(current.descriptor.0, &mut control, &mut revision) }
        == 0
    {
        return Err(io::Error::last_os_error());
    }
    let Some(dacl) = current.dacl() else {
        return Ok(false);
    };
    Ok(control & SE_DACL_PROTECTED != 0 && same_aces(dacl, expected.dacl()?))
}

fn set_dacl(path: &Path, descriptor: &SecurityDescriptor) -> io::Result<()> {
    let name = wide_path(path);
    let status = unsafe {
        SetNamedSecurityInfoW(
            name.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            descriptor.dacl()?,
            null_mut(),
        )
    };
    win32_result(status)
}

struct NamedDacl {
    descriptor: SecurityDescriptor,
    dacl: *mut ACL,
}

impl NamedDacl {
    /// The object's DACL, or `None` for a NULL DACL (unrestricted access).
    fn dacl(&self) -> Option<&ACL> {
        // SAFETY: a non-null DACL points into `self.descriptor`, which
        // outlives the borrow.
        unsafe { self.dacl.as_ref() }
    }

    fn read(path: &Path) -> io::Result<Self> {
        let name = wide_path(path);
        let mut dacl = null_mut();
        let mut descriptor = null_mut();
        let status = unsafe {
            GetNamedSecurityInfoW(
                name.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        };
        win32_result(status)?;
        Ok(Self {
            descriptor: SecurityDescriptor(descriptor),
            dacl,
        })
    }
}

/// Compare two ACLs entry by entry, ignoring allocation slack in their headers.
fn same_aces(left: &ACL, right: &ACL) -> bool {
    left.AceCount == right.AceCount
        && (0..u32::from(left.AceCount)).all(|index| {
            matches!(
                (ace_bytes(left, index), ace_bytes(right, index)),
                (Some(left), Some(right)) if left == right
            )
        })
}

/// The raw bytes of one access control entry, borrowed from its ACL.
fn ace_bytes(acl: &ACL, index: u32) -> Option<&[u8]> {
    let mut ace: *mut c_void = null_mut();
    if unsafe { GetAce(acl, index, &mut ace) } == 0 {
        return None;
    }
    // SAFETY: on success `ace` points at an entry inside `acl`, beginning with
    // an ACE_HEADER whose AceSize covers the whole entry.
    let header = unsafe { ace.cast::<ACE_HEADER>().as_ref() }?;
    Some(unsafe { std::slice::from_raw_parts(ace.cast::<u8>(), usize::from(header.AceSize)) })
}

pub(crate) fn current_user_sid() -> io::Result<String> {
    let mut token: HANDLE = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let result = token_user_sid(token);
    unsafe { CloseHandle(token) };
    result
}

fn token_user_sid(token: HANDLE) -> io::Result<String> {
    let mut length = 0;
    unsafe { GetTokenInformation(token, TokenUser, null_mut(), 0, &mut length) };
    // `u64` storage keeps TOKEN_USER's pointer field aligned.
    let mut buffer = vec![0u64; (length as usize).div_ceil(8)];
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            length,
            &mut length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut sid = null_mut();
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let text = unsafe { from_wide(sid) };
    unsafe { LocalFree(sid.cast()) };
    Ok(text)
}

fn wide(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(std::iter::once(0)).collect()
}

fn wide_path(path: &Path) -> Vec<u16> {
    let mut value = path.as_os_str().encode_wide().collect::<Vec<_>>();
    let prefix = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
    if path.is_absolute() && value.len() > 240 && !value.starts_with(&prefix) {
        if value.starts_with(&[b'\\' as u16, b'\\' as u16]) {
            value.drain(..2);
            value.splice(0..0, "\\\\?\\UNC\\".encode_utf16());
        } else {
            value.splice(0..0, prefix);
        }
    }
    value.push(0);
    value
}

unsafe fn from_wide(value: *const u16) -> String {
    let length = (0..).take_while(|&index| *value.add(index) != 0).count();
    String::from_utf16_lossy(std::slice::from_raw_parts(value, length))
}

fn win32_result(status: u32) -> io::Result<()> {
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

/// Independent SDDL-based inspection, so tests verify the resulting ACLs
/// without trusting the production comparison above.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::os::windows::io::RawHandle;
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetSecurityInfo, SE_KERNEL_OBJECT,
    };
    use windows_sys::Win32::Security::OWNER_SECURITY_INFORMATION;

    /// Install an arbitrary DACL, e.g. to simulate a shared location.
    pub(crate) fn set_sddl(path: &Path, sddl: &str) {
        let sddl = sddl.replace("{user}", &current_user_sid().unwrap());
        set_dacl(path, &SecurityDescriptor::from_sddl(&sddl).unwrap()).unwrap();
    }

    pub(crate) fn path_dacl_sddl(path: &Path) -> String {
        let named = NamedDacl::read(path).unwrap();
        descriptor_sddl(&named.descriptor, DACL_SECURITY_INFORMATION)
    }

    pub(crate) fn handle_dacl_sddl(handle: RawHandle) -> String {
        let mut dacl = null_mut();
        let mut descriptor = null_mut();
        // Pipe handles are kernel objects; SE_FILE_OBJECT is rejected.
        let status = unsafe {
            GetSecurityInfo(
                handle,
                SE_KERNEL_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        };
        win32_result(status).unwrap();
        descriptor_sddl(&SecurityDescriptor(descriptor), DACL_SECURITY_INFORMATION)
    }

    fn descriptor_sddl(descriptor: &SecurityDescriptor, information: u32) -> String {
        let mut text = null_mut();
        let ok = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor.0,
                SDDL_REVISION_1,
                information,
                &mut text,
                null_mut(),
            )
        };
        assert_ne!(ok, 0, "{}", io::Error::last_os_error());
        let sddl = unsafe { from_wide(text) };
        unsafe { LocalFree(text.cast()) };
        sddl
    }

    /// How SDDL renders the current user; well-known accounts use aliases.
    fn current_user_sddl() -> String {
        let sid = current_user_sid().unwrap();
        let descriptor = SecurityDescriptor::from_sddl(&format!("O:{sid}")).unwrap();
        descriptor_sddl(&descriptor, OWNER_SECURITY_INFORMATION)
            .trim_start_matches("O:")
            .to_owned()
    }

    fn dacl_aces(sddl: &str) -> (&str, Vec<Vec<&str>>) {
        let dacl = sddl
            .split_once("D:")
            .unwrap_or_else(|| panic!("no DACL in {sddl}"))
            .1;
        let (flags, aces) = dacl.split_once('(').unwrap_or((dacl, ""));
        let aces = aces
            .trim_end_matches(')')
            .split(")(")
            .filter(|ace| !ace.is_empty())
            .map(|ace| ace.split(';').collect())
            .collect();
        (flags, aces)
    }

    /// Assert that a DACL grants access only to the current user and SYSTEM.
    pub(crate) fn assert_only_owner_access(sddl: &str) {
        let user = current_user_sddl();
        let (_, aces) = dacl_aces(sddl);
        assert!(
            aces.iter().any(|ace| ace.get(5) == Some(&user.as_str())),
            "current user {user} is not granted access: {sddl}"
        );
        for ace in aces {
            assert_eq!(ace.len(), 6, "malformed ACE in {sddl}");
            assert_eq!(ace[0], "A", "unexpected ACE type in {sddl}");
            assert!(
                ace[5] == user || ace[5] == "SY",
                "{} is granted access: {sddl}",
                ace[5]
            );
        }
    }

    /// Assert that a DACL is protected from inheritance and grants access
    /// only to the current user and SYSTEM.
    pub(crate) fn assert_owner_only(sddl: &str, inheritable: bool) {
        assert_only_owner_access(sddl);
        let (flags, aces) = dacl_aces(sddl);
        assert!(flags.contains('P'), "DACL is not protected: {sddl}");
        if inheritable {
            for ace in aces {
                assert!(
                    ace[1].contains("OI") && ace[1].contains("CI"),
                    "ACE is not inherited by children: {sddl}"
                );
            }
        }
    }
}
