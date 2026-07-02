use std::fs::Metadata;
#[cfg(unix)]
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use tokio::fs::OpenOptions;
use tracing::debug;

use crate::nfs::*;

/// Compares if file metadata has changed in a significant way
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn metadata_differ(lhs: &Metadata, rhs: &Metadata) -> bool {
    lhs.ino() != rhs.ino() || lhs.mtime() != rhs.mtime() || lhs.len() != rhs.len() || lhs.file_type() != rhs.file_type()
}

/// Windows version: no inode, uses modified() instead of mtime()
#[cfg(target_os = "windows")]
pub fn metadata_differ(lhs: &Metadata, rhs: &Metadata) -> bool {
    let mtime_differ = lhs.modified().ok() != rhs.modified().ok();
    lhs.len() != rhs.len() || lhs.file_type() != rhs.file_type() || mtime_differ
}
pub fn fattr3_differ(lhs: &fattr3, rhs: &fattr3) -> bool {
    lhs.fileid != rhs.fileid
        || lhs.mtime.seconds != rhs.mtime.seconds
        || lhs.mtime.nseconds != rhs.mtime.nseconds
        || lhs.size != rhs.size
        || lhs.ftype as u32 != rhs.ftype as u32
}

/// Cross-platform conversion from SystemTime to NFS nfstime3
fn system_time_to_nfstime(st: std::io::Result<std::time::SystemTime>) -> nfstime3 {
    let dur = st
        .unwrap_or(std::time::UNIX_EPOCH)
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    nfstime3 {
        seconds: dur.as_secs() as u32,
        nseconds: dur.subsec_nanos(),
    }
}

/// path.exists() is terrifyingly unsafe as that
/// traverses symlinks. This can cause deadlocks if we have a
/// recursive symlink.
pub fn exists_no_traverse(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

#[cfg(unix)]
fn mode_unmask(mode: u32) -> u32 {
    // it is possible to create a file we cannot write to.
    // we force writable always.
    let mode = mode | 0x80;
    let mode = Permissions::from_mode(mode);
    mode.mode() & 0x1FF
}

#[cfg(windows)]
#[allow(dead_code)]
fn mode_unmask(_mode: u32) -> u32 {
    0o777
}

/// Converts fs Metadata to NFS fattr3
pub fn metadata_to_fattr3(fid: fileid3, meta: &Metadata) -> fattr3 {
    let size = meta.len();

    #[cfg(unix)]
    let (uid, gid, mode) = {
        use std::os::unix::fs::MetadataExt;
        (meta.uid(), meta.gid(), mode_unmask(meta.mode()))
    };
    #[cfg(windows)]
    let (uid, gid, mode) = (0, 0, 0o777);

    let atime = system_time_to_nfstime(meta.accessed());
    let mtime = system_time_to_nfstime(meta.modified());
    let ctime = system_time_to_nfstime(meta.created());

    let (ftype, nlink) = if meta.is_file() {
        (ftype3::NF3REG, 1)
    } else if meta.is_symlink() {
        (ftype3::NF3LNK, 1)
    } else {
        (ftype3::NF3DIR, 2)
    };

    fattr3 {
        ftype,
        mode,
        nlink,
        uid,
        gid,
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: 0,
        fileid: fid,
        atime,
        mtime,
        ctime,
    }
}

/// Set attributes of a path
pub async fn path_setattr(path: &Path, setattr: &sattr3) -> Result<(), nfsstat3> {
    match setattr.atime {
        set_atime::SET_TO_SERVER_TIME => {
            let _ = filetime::set_file_atime(path, filetime::FileTime::now());
        },
        set_atime::SET_TO_CLIENT_TIME(time) => {
            let _ = filetime::set_file_atime(path, time.into());
        },
        _ => {},
    };
    match setattr.mtime {
        set_mtime::SET_TO_SERVER_TIME => {
            let _ = filetime::set_file_mtime(path, filetime::FileTime::now());
        },
        set_mtime::SET_TO_CLIENT_TIME(time) => {
            let _ = filetime::set_file_mtime(path, time.into());
        },
        _ => {},
    };
    #[cfg(unix)]
    if let set_mode3::mode(mode) = setattr.mode {
        debug!(" -- set permissions {:?} {:?}", path, mode);
        let mode = mode_unmask(mode);
        let _ = std::fs::set_permissions(path, Permissions::from_mode(mode));
    };
    if let set_uid3::uid(_) = setattr.uid {
        debug!("Set uid not implemented");
    }
    if let set_gid3::gid(_) = setattr.gid {
        debug!("Set gid not implemented");
    }
    if let set_size3::size(size3) = setattr.size {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .await
            .or(Err(nfsstat3::NFS3ERR_IO))?;
        debug!(" -- set size {:?} {:?}", path, size3);
        file.set_len(size3).await.or(Err(nfsstat3::NFS3ERR_IO))?;
    }
    Ok(())
}

/// Set attributes of a file
pub async fn file_setattr(file: &std::fs::File, setattr: &sattr3) -> Result<(), nfsstat3> {
    #[cfg(unix)]
    if let set_mode3::mode(mode) = setattr.mode {
        debug!(" -- set permissions {:?}", mode);
        let mode = mode_unmask(mode);
        let _ = file.set_permissions(Permissions::from_mode(mode));
    }
    if let set_size3::size(size3) = setattr.size {
        debug!(" -- set size {:?}", size3);
        file.set_len(size3).or(Err(nfsstat3::NFS3ERR_IO))?;
    }
    Ok(())
}
