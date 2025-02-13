use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileTypeExt;

#[derive(Debug)]
pub struct Device {
    fp: std::fs::File, // buffered reader/writer ?
    mode: crate::option::OpenMode,
    size: u64, // in bytes
    #[cfg(not(target_os = "linux"))] // FreeBSD
    blksize: u64,
}

impl Device {
    /// # Errors
    pub fn new(spec: &str, mode: &str) -> crate::Result<Self> {
        Self::new_impl(
            spec,
            match mode {
                "rw" => crate::option::OpenMode::Rw,
                "ro" => crate::option::OpenMode::Ro,
                "any" => crate::option::OpenMode::Any, // "ro_fallback" in relan/exfat
                _ => return Err(nix::errno::Errno::EINVAL.into()),
            },
        )
    }

    pub(crate) fn new_impl(spec: &str, mode: crate::option::OpenMode) -> crate::Result<Self> {
        open(spec, mode)
    }

    /// # Errors
    pub fn fsync(&mut self) -> std::io::Result<()> {
        self.fp.flush()
    }

    pub(crate) fn get_mode(&self) -> crate::option::OpenMode {
        self.mode
    }

    #[must_use]
    pub fn get_size(&self) -> u64 {
        self.size
    }

    #[cfg(not(target_os = "linux"))]
    fn get_aligned_range(&self, buf: &[u8], offset: u64) -> (u64, u64) {
        let beg = crate::util::round_down!(offset, self.blksize);
        let end = crate::util::round_up!(offset + u64::try_from(buf.len()).unwrap(), self.blksize);
        assert!(offset >= beg);
        assert_eq!((end - beg) % self.blksize, 0);
        (beg, end)
    }

    #[cfg(target_os = "linux")]
    /// # Errors
    pub fn pread(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        crate::util::seek_set(&mut self.fp, offset)?;
        self.fp.read_exact(buf)
    }

    #[cfg(not(target_os = "linux"))]
    /// # Errors
    /// # Panics
    pub fn pread(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        let (beg, end) = self.get_aligned_range(buf, offset);
        let mut lbuf = vec![0; (end - beg).try_into().unwrap()];
        crate::util::seek_set(&mut self.fp, beg)?;
        self.fp.read_exact(&mut lbuf)?;
        let x = (offset - beg).try_into().unwrap();
        buf.copy_from_slice(&lbuf[x..x + buf.len()]);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    /// # Errors
    pub fn pwrite(&mut self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        crate::util::seek_set(&mut self.fp, offset)?;
        self.fp.write_all(buf)
    }

    #[cfg(not(target_os = "linux"))]
    /// # Errors
    /// # Panics
    pub fn pwrite(&mut self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        let (beg, end) = self.get_aligned_range(buf, offset);
        let mut lbuf = vec![0; (end - beg).try_into().unwrap()];
        crate::util::seek_set(&mut self.fp, beg)?;
        self.fp.read_exact(&mut lbuf)?;
        let x = (offset - beg).try_into().unwrap();
        lbuf[x..x + buf.len()].copy_from_slice(buf);
        crate::util::seek_set(&mut self.fp, beg)?;
        self.fp.write_all(&lbuf)
    }

    /// # Errors
    /// # Panics
    pub fn preadx(&mut self, size: u64, offset: u64) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0; size.try_into().unwrap()];
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

fn open_ro(spec: &str) -> crate::Result<std::fs::File> {
    Ok(std::fs::File::open(spec)?)
}

fn open_rw(spec: &str) -> crate::Result<std::fs::File> {
    let fp = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(spec)?;

    if crate::util::is_linux() {
        // linux/fs.h:#define BLKROGET   _IO(0x12,94) /* get read-only status (0 = read_write) */
        nix::ioctl_read_bad!(blkroget, 0x125e, u32);

        // This ioctl is needed because after "blockdev --setro" kernel still
        // allows to open the device in read-write mode but fails writes.
        let mut ro = 0;
        if let Ok(v) = unsafe { blkroget(fp.as_raw_fd(), &mut ro) } {
            if v == 0 {
                if ro != 0 {
                    return Err(nix::errno::Errno::EROFS.into());
                }
            } else {
                return Err(nix::errno::Errno::EINVAL.into());
            }
        }
    }
    Ok(fp)
}

fn open(spec: &str, mode: crate::option::OpenMode) -> crate::Result<Device> {
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
        crate::option::OpenMode::Rw => (open_rw(spec)?, mode),
        crate::option::OpenMode::Ro => (open_ro(spec)?, mode),
        crate::option::OpenMode::Any => {
            if let Ok(v) = open_rw(spec) {
                (v, crate::option::OpenMode::Rw)
            } else {
                log::warn!("'{spec}' is write-protected, opening read-only");
                (open_ro(spec)?, crate::option::OpenMode::Ro)
            }
        }
    };

    let t = fp.metadata()?.file_type();
    if !t.is_block_device() && !t.is_char_device() && !t.is_file() {
        log::error!("'{spec}' is neither a device, nor a regular file");
        return Err(nix::errno::Errno::EINVAL.into());
    }

    let size = if crate::util::is_linux() || crate::util::is_freebsd() || crate::util::is_solaris()
    {
        let size = crate::util::seek_end(&mut fp, 0)?;
        if size == 0 {
            log::error!("failed to get size of '{spec}'");
            return Err(nix::errno::Errno::EINVAL.into());
        }
        crate::util::seek_set(&mut fp, 0)?;
        size
    } else {
        // XXX other platforms use ioctl(2)
        log::error!("{} is unsupported", crate::util::get_os_name());
        return Err(nix::errno::Errno::EOPNOTSUPP.into());
    };
    Ok(Device {
        fp,
        mode,
        size,
        #[cfg(not(target_os = "linux"))]
        blksize: 512, // XXX use ioctl(2)
    })
}
