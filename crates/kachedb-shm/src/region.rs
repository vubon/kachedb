//! `kachedb-shm` — Cross-platform POSIX shared memory region abstraction.
//!
//! Maps a named POSIX shared memory object into the current process's virtual
//! address space. Python workers attach to the same name to achieve zero-copy
//! tensor transfer.
//!
//! # Platform Dispatch
//!
//! | Platform | Method | Path |
//! |---|---|---|
//! | Linux | Direct open on `/dev/shm/<name>` with `MAP_POPULATE` | `/dev/shm/kachedb_0` |
//! | macOS | `shm_open()` + `mmap` | OS-managed SHM namespace |

use std::ptr::NonNull;

use crate::error::ShmError;

/// A memory-mapped shared memory region.
///
/// On drop, the `mmap` mapping is released (`munmap`) and the underlying
/// shared memory object may be unlinked if `owner` is set to `true`.
pub struct ShmRegion {
    /// Base pointer to the mapped memory.
    pub ptr: NonNull<u8>,
    /// Total byte size of the mapped region.
    pub size: usize,
    /// The POSIX name used to identify this shared memory object.
    pub name: String,
    /// If `true`, this process created the region and will `shm_unlink` on drop.
    owner: bool,
}

// SAFETY: The memory is exclusively accessible within this process's mapping.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl ShmRegion {
    /// Opens or creates a POSIX shared memory region of `size_bytes`.
    ///
    /// If the region does not exist, it is created and sized to `size_bytes`.
    /// If it already exists (e.g., opened by a Python worker), it is attached.
    ///
    /// Set `owner = true` when this process creates the region; it will then
    /// call `shm_unlink` on drop to clean up the OS namespace entry.
    pub fn open_or_create(
        name: &str,
        size_bytes: usize,
        owner: bool,
    ) -> Result<Self, ShmError> {
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                Self::open_linux(name, size_bytes, owner)
            } else {
                Self::open_posix(name, size_bytes, owner)
            }
        }
    }

    /// Returns the base pointer as a raw `*mut u8`.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns a raw pointer cast to `*mut T`.
    ///
    /// # Safety
    ///
    /// Caller must ensure `T` fits within `self.size` bytes and the alignment
    /// of the mapping satisfies `T`'s alignment requirement.
    #[inline]
    pub unsafe fn as_typed_ptr<T>(&self) -> *mut T {
        self.ptr.as_ptr() as *mut T
    }

    // ── Linux fast-path: direct /dev/shm open ────────────────────────────────

    #[cfg(target_os = "linux")]
    fn open_linux(name: &str, size_bytes: usize, owner: bool) -> Result<Self, ShmError> {
        use std::fs::OpenOptions;
        use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};

        let path = format!("/dev/shm/{name}");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o660)
            .open(&path)
            .map_err(|e| ShmError::OpenFailed {
                name: name.to_string(),
                reason: e.to_string(),
            })?;

        file.set_len(size_bytes as u64).map_err(|e| ShmError::ResizeFailed {
            name: name.to_string(),
            size: size_bytes,
            reason: e.to_string(),
        })?;

        let raw_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE, // Pre-fault all page-table entries.
                file.as_raw_fd(),
                0,
            )
        };

        Self::from_raw_ptr(raw_ptr, size_bytes, name, owner)
    }

    // ── macOS / POSIX fallback: shm_open ─────────────────────────────────────

    #[cfg(not(target_os = "linux"))]
    fn open_posix(name: &str, size_bytes: usize, owner: bool) -> Result<Self, ShmError> {
        use std::ffi::CString;

        let shm_name = format!("/{name}");
        let c_name = CString::new(shm_name).unwrap();

        // Create (owner) or attach (non-owner) with the appropriate flags.
        let flags = if owner {
            libc::O_RDWR | libc::O_CREAT
        } else {
            libc::O_RDWR
        };

        let fd = unsafe { libc::shm_open(c_name.as_ptr(), flags, 0o660) };

        if fd < 0 {
            return Err(ShmError::OpenFailed {
                name: name.to_string(),
                reason: std::io::Error::last_os_error().to_string(),
            });
        }

        // Only resize when we are the creator; attaching to an existing region
        // with ftruncate fails with EINVAL on macOS.
        if owner {
            let trunc_res = unsafe { libc::ftruncate(fd, size_bytes as libc::off_t) };
            if trunc_res < 0 {
                unsafe { libc::close(fd) };
                return Err(ShmError::ResizeFailed {
                    name: name.to_string(),
                    size: size_bytes,
                    reason: std::io::Error::last_os_error().to_string(),
                });
            }
        }

        let raw_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        unsafe { libc::close(fd) };
        Self::from_raw_ptr(raw_ptr, size_bytes, name, owner)
    }

    // ── Shared helper ─────────────────────────────────────────────────────────

    fn from_raw_ptr(
        raw_ptr: *mut libc::c_void,
        size: usize,
        name: &str,
        owner: bool,
    ) -> Result<Self, ShmError> {
        if raw_ptr == libc::MAP_FAILED || raw_ptr.is_null() {
            return Err(ShmError::MmapFailed {
                name: name.to_string(),
                reason: std::io::Error::last_os_error().to_string(),
            });
        }

        let ptr = NonNull::new(raw_ptr as *mut u8).ok_or_else(|| ShmError::MmapFailed {
            name: name.to_string(),
            reason: "mmap returned null".into(),
        })?;

        log::debug!(
            "ShmRegion: mapped '{name}' at {:p} ({size} bytes, owner={owner})",
            ptr.as_ptr()
        );

        Ok(Self { ptr, size, name: name.to_string(), owner })
    }
}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // Unmap the virtual address range.
        unsafe { libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.size) };

        // If this process created the region, remove it from the OS namespace.
        if self.owner {
            cfg_if::cfg_if! {
                if #[cfg(target_os = "linux")] {
                    let path = format!("/dev/shm/{}", self.name);
                    let _ = std::fs::remove_file(&path);
                } else {
                    use std::ffi::CString;
                    let shm_name = format!("/{}", self.name);
                    if let Ok(c) = CString::new(shm_name) {
                        unsafe { libc::shm_unlink(c.as_ptr()) };
                    }
                }
            }
        }

        log::debug!("ShmRegion: unmapped '{}'", self.name);
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_write_shm_region() {
        let name = format!("kachedb_test_{}", std::process::id());
        let region = ShmRegion::open_or_create(&name, 4096, true)
            .expect("ShmRegion should be created");

        // Write a magic value into the shared memory and read it back.
        unsafe {
            let ptr = region.as_ptr();
            ptr.write(0xAB);
            assert_eq!(ptr.read(), 0xAB);
        }
        // Drop cleans up automatically.
    }

    #[test]
    fn attach_to_existing_shm_region() {
        let name = format!("kachedb_attach_{}", std::process::id());
        let owner = ShmRegion::open_or_create(&name, 4096, true).unwrap();
        // Second mapping (non-owner) attaches to the same region.
        let reader = ShmRegion::open_or_create(&name, 4096, false).unwrap();

        unsafe {
            owner.as_ptr().write(0xCC);
            assert_eq!(reader.as_ptr().read(), 0xCC);
        }
        drop(reader);
        drop(owner); // owner unlinks on drop
    }
}
