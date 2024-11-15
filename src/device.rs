use crate::option;
use crate::util;

use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileTypeExt;

#[derive(Debug)]
pub struct ExfatDevice {
    fp: std::fs::File, // buffered reader/writer ?
    mode: option::ExfatMode,
    size: u64, // in bytes
    #[cfg(not(target_os = "linux"))] // FreeBSD
    blksize: u64,
}

impl ExfatDevice {
    /// # Errors
    pub fn new(spec: &str, mode: &str) -> std::io::Result<Self> {
        Self::new_from_opt(
            spec,
            match mode {
                "rw" => option::ExfatMode::Rw,
                "ro" => option::ExfatMode::Ro,
                "any" => option::ExfatMode::Any, // "ro_fallback" in relan/exfat
                _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
            },
        )
    }

    pub(crate) fn new_from_opt(spec: &str, mode: option::ExfatMode) -> std::io::Result<Self> {
        open(spec, mode)
    }

    /// # Errors
    pub fn fsync(&mut self) -> std::io::Result<()> {
        self.fp.flush()
    }

    pub(crate) fn get_mode(&self) -> option::ExfatMode {
        self.mode
    }

    #[must_use]
    pub fn get_size(&self) -> u64 {
        self.size
    }

    #[cfg(not(target_os = "linux"))]
    fn get_aligned_range(&self, buf: &[u8], offset: u64) -> (u64, u64) {
        let beg = util::round_down!(offset, self.blksize);
        let end = util::round_up!(offset + u64::try_from(buf.len()).unwrap(), self.blksize);
        assert!(offset >= beg);
        assert_eq!((end - beg) % self.blksize, 0);
        (beg, end)
    }

    #[cfg(target_os = "linux")]
    /// # Errors
    pub fn pread(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        let cur = util::seek_cur(&mut self.fp, 0)?;
        util::seek_set(&mut self.fp, offset)?;
        let result = self.fp.read_exact(buf);
        util::seek_set(&mut self.fp, cur)?;
        result
    }

    #[cfg(not(target_os = "linux"))]
    /// # Errors
    /// # Panics
    pub fn pread(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        let cur = util::seek_cur(&mut self.fp, 0)?;
        let (beg, end) = self.get_aligned_range(buf, offset);
        let mut lbuf = vec![0; (end - beg).try_into().unwrap()];
        util::seek_set(&mut self.fp, beg)?;
        let result = self.fp.read_exact(&mut lbuf);
        util::seek_set(&mut self.fp, cur)?;
        if result.is_ok() {
            let x = (offset - beg).try_into().unwrap();
            buf.copy_from_slice(&lbuf[x..x + buf.len()]);
        }
        result
    }

    #[cfg(target_os = "linux")]
    /// # Errors
    pub fn pwrite(&mut self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        let cur = util::seek_cur(&mut self.fp, 0)?;
        util::seek_set(&mut self.fp, offset)?;
        let result = self.fp.write_all(buf);
        util::seek_set(&mut self.fp, cur)?;
        result
    }

    #[cfg(not(target_os = "linux"))]
    /// # Errors
    /// # Panics
    pub fn pwrite(&mut self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        let cur = util::seek_cur(&mut self.fp, 0)?;
        let (beg, end) = self.get_aligned_range(buf, offset);
        let mut lbuf = vec![0; (end - beg).try_into().unwrap()];
        util::seek_set(&mut self.fp, beg)?;
        self.fp.read_exact(&mut lbuf)?;
        let x = (offset - beg).try_into().unwrap();
        lbuf[x..x + buf.len()].copy_from_slice(buf);
        util::seek_set(&mut self.fp, beg)?;
        let result = self.fp.write_all(&lbuf);
        util::seek_set(&mut self.fp, cur)?;
        result
    }

    /// # Errors
    pub fn preadx(&mut self, size: u64, offset: u64) -> std::io::Result<Vec<u8>> {
        let size = match size.try_into() {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
            }
        };
        let mut buf = vec![0; size];
        self.pread(&mut buf, offset)?;
        Ok(buf)
    }
}

fn is_open(fd: std::os::fd::RawFd) -> bool {
    if let Ok(v) = nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETFD) {
        v == 0
    } else {
        false
    }
}

fn open_ro(spec: &str) -> std::io::Result<std::fs::File> {
    std::fs::File::open(spec)
}

fn open_rw(spec: &str) -> std::io::Result<std::fs::File> {
    let fp = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(spec)?;

    if util::is_linux() {
        // linux/fs.h:#define BLKROGET   _IO(0x12,94) /* get read-only status (0 = read_write) */
        nix::ioctl_read_bad!(blkroget, 0x125e, u32);

        // This ioctl is needed because after "blockdev --setro" kernel still
        // allows to open the device in read-write mode but fails writes.
        let mut ro = 0;
        if let Ok(v) = unsafe { blkroget(fp.as_raw_fd(), &mut ro) } {
            if v == 0 {
                if ro != 0 {
                    // want ReadOnlyFilesystem
                    return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
                }
            } else {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
            }
        }
    }
    Ok(fp)
}

fn open(spec: &str, mode: option::ExfatMode) -> std::io::Result<ExfatDevice> {
    // The system allocates file descriptors sequentially. If we have been
    // started with stdin (0), stdout (1) or stderr (2) closed, the system
    // will give us descriptor 0, 1 or 2 later when we open block device,
    // FUSE communication pipe, etc. As a result, functions using stdin,
    // stdout or stderr will actually work with a different thing and can
    // corrupt it. Protect descriptors 0, 1 and 2 from such misuse.
    while !is_open(0) || !is_open(1) || !is_open(2) {
        // we don't need those descriptors, let them leak
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
    }

    let (mut fp, mode) = match mode {
        option::ExfatMode::Rw => (open_rw(spec)?, mode),
        option::ExfatMode::Ro => (open_ro(spec)?, mode),
        option::ExfatMode::Any => {
            if let Ok(v) = open_rw(spec) {
                (v, option::ExfatMode::Rw)
            } else {
                log::warn!("'{spec}' is write-protected, opening read-only");
                (open_ro(spec)?, option::ExfatMode::Ro)
            }
        }
    };

    let t = fp.metadata()?.file_type();
    if !t.is_block_device() && !t.is_char_device() && !t.is_file() {
        log::error!("'{spec}' is neither a device, nor a regular file");
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }

    let size = if util::is_linux() || util::is_freebsd() || util::is_solaris() {
        let size = util::seek_end(&mut fp, 0)?;
        if size == 0 {
            log::error!("failed to get size of '{spec}'");
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }
        util::seek_set(&mut fp, 0)?;
        size
    } else {
        // XXX other platforms use ioctl(2)
        log::error!("{} is unsupported", util::get_os_name());
        return Err(std::io::Error::from(std::io::ErrorKind::Unsupported));
    };
    Ok(ExfatDevice {
        fp,
        mode,
        size,
        #[cfg(not(target_os = "linux"))]
        blksize: 512, // XXX use ioctl(2)
    })
}
