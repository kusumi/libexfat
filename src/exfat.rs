use crate::bitmap;
use crate::device;
use crate::exfatfs;
use crate::node;
use crate::option;
use crate::time;
use crate::utf;
use crate::util;

use byteorder::ByteOrder;
use std::io::Write;

macro_rules! get_node {
    ($nmap:expr, $nid:expr) => {
        $nmap.get($nid).unwrap()
    };
}
pub(crate) use get_node;

macro_rules! get_mut_node {
    ($nmap:expr, $nid:expr) => {
        $nmap.get_mut($nid).unwrap()
    };
}
pub(crate) use get_mut_node;

macro_rules! error_or_panic {
    ($msg:expr, $debug:expr) => {
        if $debug {
            panic!("{}", $msg);
        } else {
            log::error!("{}", $msg);
        }
    };
}

pub const EXFAT_NAME_MAX: usize = 255;

// UTF-16 encodes code points up to U+FFFF as single 16-bit code units.
// UTF-8 uses up to 3 bytes (i.e. 8-bit code units) to encode code points
// up to U+FFFF. relan/exfat has +1 for NULL termination.
pub(crate) const EXFAT_UTF8_NAME_BUFFER_MAX: usize = EXFAT_NAME_MAX * 3;
pub(crate) const EXFAT_UTF8_ENAME_BUFFER_MAX: usize = exfatfs::EXFAT_ENAME_MAX * 3;

#[cfg(target_os = "linux")]
pub type ExfatStatMode = u32;
#[cfg(not(target_os = "linux"))]
pub type ExfatStatMode = u16;

#[derive(Debug)]
pub struct ExfatStat {
    pub st_dev: u64,
    pub st_ino: u64,
    pub st_nlink: u32,
    pub st_mode: ExfatStatMode,
    pub st_uid: u32,
    pub st_gid: u32,
    pub st_rdev: u32,
    pub st_size: u64,
    pub st_blksize: u32,
    pub st_blocks: u64,
    pub st_atime: u64,
    pub st_mtime: u64,
    pub st_ctime: u64,
}

#[derive(Debug)]
pub struct ExfatStatFs {
    pub f_bsize: u32,
    pub f_blocks: u64,
    pub f_bfree: u64,
    pub f_bavail: u64,
    pub f_files: u64,
    pub f_ffree: u64,
    pub f_namelen: u32,
    pub f_frsize: u32,
}

#[derive(Debug)]
pub struct ExfatCursor {
    pnid: node::Nid,
    curnid: node::Nid,
    curidx: usize,
}

impl ExfatCursor {
    fn new(pnid: node::Nid) -> Self {
        Self {
            pnid,
            curnid: node::NID_INVALID,
            curidx: usize::MAX,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ExfatClusterMap {
    start_cluster: u32,
    size: u32, // in bits
    pub(crate) chunk: Vec<bitmap::Bitmap>,
    chunk_size: u32, // in bits
    dirty: bool,
}

impl ExfatClusterMap {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub struct Exfat {
    opt: option::ExfatOption, // Rust
    pub(crate) dev: device::ExfatDevice,
    pub(crate) sb: exfatfs::ExfatSuperBlock,
    upcase: Vec<u16>,
    pub(crate) cmap: ExfatClusterMap,
    pub(crate) strlabel: String,
    zero_cluster: Vec<u8>,
    pub(crate) ro: isize,
    pub(crate) errors: usize,       // global variable in relan/exfat
    pub(crate) errors_fixed: usize, // global variable in relan/exfat
    pub(crate) nid_next: node::Nid, // Rust (should be bitmamp, but large enough)
    pub(crate) nmap: std::collections::HashMap<node::Nid, node::ExfatNode>, // Rust
}

impl Drop for Exfat {
    fn drop(&mut self) {
        if !self.nmap.is_empty() {
            log::debug!("unmount '{}' on drop", self.strlabel);
            assert!(self.nmap.contains_key(&node::NID_ROOT));
            self.unmount().unwrap();
        }
    }
}

impl Exfat {
    pub(crate) fn new(dev: device::ExfatDevice, opt: option::ExfatOption) -> Self {
        Self {
            opt,
            dev,
            sb: exfatfs::ExfatSuperBlock::new(),
            upcase: vec![],
            cmap: ExfatClusterMap::new(),
            strlabel: String::new(),
            zero_cluster: vec![],
            ro: 0,
            errors: 0,
            errors_fixed: 0,
            nid_next: node::NID_ROOT + 1,
            nmap: std::collections::HashMap::new(),
        }
    }

    // Sector to absolute offset.
    fn s2o(&self, sector: u64) -> u64 {
        sector << self.sb.sector_bits
    }

    // Cluster to sector.
    fn c2s(&self, cluster: u32) -> u64 {
        assert!(
            cluster >= exfatfs::EXFAT_FIRST_DATA_CLUSTER,
            "invalid cluster number {cluster}"
        );
        u64::from_le(self.sb.cluster_sector_start.into())
            + (u64::from(cluster - exfatfs::EXFAT_FIRST_DATA_CLUSTER) << self.sb.spc_bits)
    }

    // Cluster to absolute offset.
    #[must_use]
    pub fn c2o(&self, cluster: u32) -> u64 {
        self.s2o(self.c2s(cluster))
    }

    // Sector to cluster.
    fn s2c(&mut self, sector: u64) -> nix::Result<u32> {
        // dumpexfat (the only user of this fn) initially passes zero,
        // and relan/exfat returns a negative cluster in uint32_t.
        // It's a bug, but usually works as the value exceeds max clusters.
        // In Rust, do extra sanity to prevent u32::try_from failure.
        let cluster_sector_start = u32::from_le(self.sb.cluster_sector_start).into();
        if sector < cluster_sector_start {
            return Ok(u32::MAX);
        }
        match u32::try_from((sector - cluster_sector_start) >> self.sb.spc_bits) {
            Ok(v) => Ok(v + exfatfs::EXFAT_FIRST_DATA_CLUSTER),
            Err(e) => {
                log::error!("{e}");
                Err(nix::errno::Errno::EINVAL)
            }
        }
    }

    // Size in bytes to size in clusters (rounded upwards).
    fn bytes2clusters(&mut self, bytes: u64) -> nix::Result<u32> {
        match util::div_round_up!(bytes, self.get_cluster_size()).try_into() {
            Ok(v) => Ok(v),
            Err(e) => {
                log::error!("{e}");
                Err(nix::errno::Errno::EFBIG) // pjdfstest/tests/ftruncate/12.t
            }
        }
    }

    #[must_use]
    pub fn cluster_invalid(&self, c: u32) -> bool {
        c < exfatfs::EXFAT_FIRST_DATA_CLUSTER
            || c - exfatfs::EXFAT_FIRST_DATA_CLUSTER >= u32::from_le(self.sb.cluster_count)
    }

    pub fn next_cluster(&mut self, nid: node::Nid, cluster: u32) -> u32 {
        assert!(
            cluster >= exfatfs::EXFAT_FIRST_DATA_CLUSTER,
            "bad cluster {cluster:#x}"
        );
        if get_node!(self.nmap, &nid).is_contiguous {
            return cluster + 1;
        }
        let fat_offset = self.s2o(u64::from_le(self.sb.fat_sector_start.into()))
            + u64::from(cluster) * exfatfs::EXFAT_CLUSTER_SIZE_U64;
        let next = match self.dev.preadx(exfatfs::EXFAT_CLUSTER_SIZE_U64, fat_offset) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return exfatfs::EXFAT_CLUSTER_BAD;
            }
        };
        u32::from_le_bytes(next.try_into().unwrap())
    }

    fn advance_cluster(&mut self, nid: node::Nid, count: u32) -> nix::Result<u32> {
        let node = get_mut_node!(self.nmap, &nid);
        if node.fptr_index > count {
            node.fptr_index = 0;
            node.fptr_cluster = node.start_cluster;
        }
        for _ in node.fptr_index..count {
            let node_fptr_cluster = self.next_cluster(nid, get_node!(self.nmap, &nid).fptr_cluster);
            get_mut_node!(self.nmap, &nid).fptr_cluster = node_fptr_cluster;
            if self.cluster_invalid(node_fptr_cluster) {
                error_or_panic!("invalid cluster {node_fptr_cluster:#x}", self.opt.debug);
                return Err(nix::errno::Errno::EIO);
            }
        }
        let node = get_mut_node!(self.nmap, &nid);
        node.fptr_index = count;
        Ok(node.fptr_cluster)
    }

    fn ffas(bitmap: &mut [bitmap::Bitmap], start: u32, end: u32) -> nix::Result<u32> {
        let index = bitmap::bmap_find_and_set(bitmap, start, end);
        if index == u32::MAX {
            Err(nix::errno::Errno::ENOSPC)
        } else {
            Ok(exfatfs::EXFAT_FIRST_DATA_CLUSTER + index)
        }
    }

    fn flush_nodes_impl(&mut self, nid: node::Nid) -> nix::Result<()> {
        let n = get_node!(self.nmap, &nid).cnids.len();
        let mut i = 0; // index access to prevent cnids.clone()
        while i < n {
            let cnid = get_node!(self.nmap, &nid).cnids[i];
            self.flush_nodes_impl(cnid)?;
            i += 1;
        }
        self.flush_node(nid)
    }

    pub fn flush_nodes(&mut self) -> nix::Result<()> {
        self.flush_nodes_impl(node::NID_ROOT)
    }

    pub fn flush(&mut self) -> nix::Result<()> {
        if self.cmap.dirty {
            if let Err(e) = self.dev.pwrite(
                &self.cmap.chunk, // XXX works only when bitmap::Bitmap is u8
                self.c2o(self.cmap.start_cluster),
            ) {
                log::error!("failed to write clusters bitmap");
                return Err(util::error2errno(e));
            }
            self.cmap.dirty = false;
        }
        Ok(())
    }

    fn set_next_cluster(&mut self, contiguous: bool, current: u32, next: u32) -> nix::Result<()> {
        if contiguous {
            return Ok(());
        }
        let fat_offset = self.s2o(u32::from_le(self.sb.fat_sector_start).into())
            + u64::from(current) * exfatfs::EXFAT_CLUSTER_SIZE_U64;
        if let Err(e) = self.dev.pwrite(&next.to_le().to_ne_bytes(), fat_offset) {
            log::error!("failed to write the next cluster {next:#x} after {current:#x}");
            return Err(util::error2errno(e));
        }
        Ok(())
    }

    fn allocate_cluster(&mut self, hint: u32) -> nix::Result<u32> {
        let mut hint = hint;
        if hint < exfatfs::EXFAT_FIRST_DATA_CLUSTER {
            hint = 0;
        } else {
            hint -= exfatfs::EXFAT_FIRST_DATA_CLUSTER;
            if hint >= self.cmap.chunk_size {
                hint = 0;
            }
        }
        let cluster = match Self::ffas(&mut self.cmap.chunk, hint, self.cmap.chunk_size) {
            Ok(v) => v,
            Err(nix::errno::Errno::ENOSPC) => match Self::ffas(&mut self.cmap.chunk, 0, hint) {
                Ok(v) => v,
                Err(nix::errno::Errno::ENOSPC) => {
                    log::error!("no free space left");
                    return Err(nix::errno::Errno::ENOSPC);
                }
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        self.cmap.dirty = true;
        Ok(cluster)
    }

    fn free_cluster(&mut self, cluster: u32) {
        assert!(
            cluster - exfatfs::EXFAT_FIRST_DATA_CLUSTER < self.cmap.size,
            "caller must check cluster validity ({:#x},{:#x})",
            cluster,
            self.cmap.size
        );
        bitmap::bmap_clr(
            &mut self.cmap.chunk,
            (cluster - exfatfs::EXFAT_FIRST_DATA_CLUSTER)
                .try_into()
                .unwrap(),
        );
        self.cmap.dirty = true;
    }

    fn make_noncontiguous(&mut self, first: u32, last: u32) -> nix::Result<()> {
        for c in first..last {
            self.set_next_cluster(false, c, c + 1)?;
        }
        Ok(())
    }

    fn grow_file(&mut self, nid: node::Nid, current: u32, difference: u32) -> nix::Result<()> {
        assert_ne!(difference, 0, "zero difference passed");
        let mut previous;
        let mut allocated = 0;
        let node = get_node!(self.nmap, &nid);

        if node.start_cluster != exfatfs::EXFAT_CLUSTER_FREE {
            // get the last cluster of the file
            previous = self.advance_cluster(nid, current - 1)?;
        } else {
            assert_eq!(
                node.fptr_index, 0,
                "non-zero pointer index {}",
                node.fptr_index
            );
            // file does not have clusters (i.e. is empty), allocate the first one for it
            previous = self.allocate_cluster(0)?;
            let node = get_mut_node!(self.nmap, &nid);
            node.fptr_cluster = previous;
            node.start_cluster = node.fptr_cluster;
            allocated = 1;
            // file consists of only one cluster, so it's contiguous
            node.is_contiguous = true;
        }

        while allocated < difference {
            let next = match self.allocate_cluster(previous + 1) {
                Ok(v) => v,
                Err(e) => {
                    if allocated != 0 {
                        if let Err(e) = self.shrink_file(nid, current + allocated, allocated) {
                            log::error!("{e}");
                        }
                    }
                    return Err(e);
                }
            };
            let node = get_node!(self.nmap, &nid);
            if next != previous + 1 && node.is_contiguous {
                // it's a pity, but we are not able to keep the file contiguous anymore
                self.make_noncontiguous(node.start_cluster, previous)?;
                let node = get_mut_node!(self.nmap, &nid);
                node.is_contiguous = false;
                node.is_dirty = true;
            }
            self.set_next_cluster(get_node!(self.nmap, &nid).is_contiguous, previous, next)?;
            previous = next;
            allocated += 1;
        }

        self.set_next_cluster(
            get_node!(self.nmap, &nid).is_contiguous,
            previous,
            exfatfs::EXFAT_CLUSTER_END,
        )
    }

    fn shrink_file(&mut self, nid: node::Nid, current: u32, difference: u32) -> nix::Result<()> {
        assert_ne!(difference, 0, "zero difference passed");
        assert_ne!(
            get_node!(self.nmap, &nid).start_cluster,
            exfatfs::EXFAT_CLUSTER_FREE,
            "unable to shrink empty file ({current} clusters)"
        );
        assert!(
            current >= difference,
            "file underflow ({current} < {difference})"
        );

        // crop the file
        let mut previous;
        if current > difference {
            let last = self.advance_cluster(nid, current - difference - 1)?;
            previous = self.next_cluster(nid, last);
            self.set_next_cluster(
                get_node!(self.nmap, &nid).is_contiguous,
                last,
                exfatfs::EXFAT_CLUSTER_END,
            )?;
        } else {
            let node = get_mut_node!(self.nmap, &nid);
            previous = node.start_cluster;
            node.start_cluster = exfatfs::EXFAT_CLUSTER_FREE;
            node.is_dirty = true;
        }
        let node = get_mut_node!(self.nmap, &nid);
        node.fptr_index = 0;
        node.fptr_cluster = node.start_cluster;

        // free remaining clusters
        let mut difference = difference;
        while difference > 0 {
            if self.cluster_invalid(previous) {
                log::error!("invalid cluster {previous:#x} while freeing after shrink");
                return Err(nix::errno::Errno::EIO);
            }
            let next = self.next_cluster(nid, previous);
            self.set_next_cluster(
                get_node!(self.nmap, &nid).is_contiguous,
                previous,
                exfatfs::EXFAT_CLUSTER_FREE,
            )?;
            self.free_cluster(previous);
            previous = next;
            difference -= 1;
        }
        Ok(())
    }

    fn erase_raw(&mut self, size: u64, offset: u64) -> nix::Result<()> {
        if let Err(e) = self
            .dev
            .pwrite(&self.zero_cluster[..size.try_into().unwrap()], offset)
        {
            log::error!("failed to erase {size} bytes at {offset}");
            return Err(util::error2errno(e));
        }
        Ok(())
    }

    fn erase_range(&mut self, nid: node::Nid, begin: u64, end: u64) -> nix::Result<()> {
        if begin >= end {
            return Ok(());
        }
        let cluster_size = self.get_cluster_size();
        let count = match (begin / cluster_size).try_into() {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return Err(nix::errno::Errno::EINVAL);
            }
        };
        let mut cluster = self.advance_cluster(nid, count)?;

        // erase from the beginning to the closest cluster boundary
        let mut cluster_boundary = (begin | (cluster_size - 1)) + 1;
        self.erase_raw(
            std::cmp::min(cluster_boundary, end) - begin,
            self.c2o(cluster) + begin % cluster_size,
        )?;

        // erase whole clusters
        while cluster_boundary < end {
            cluster = self.next_cluster(nid, cluster);
            // the cluster cannot be invalid because we have just allocated it
            assert!(
                !self.cluster_invalid(cluster),
                "invalid cluster {cluster:#x} after allocation"
            );
            self.erase_raw(cluster_size, self.c2o(cluster))?;
            cluster_boundary += cluster_size;
        }
        Ok(())
    }

    pub fn truncate(&mut self, nid: node::Nid, size: u64, erase: bool) -> nix::Result<()> {
        let node = get_node!(self.nmap, &nid);
        assert!(
            node.references != 0 || node.pnid == node::NID_INVALID,
            "no references, node changes can be lost, pnid {}",
            node.pnid
        );
        if node.size == size {
            return Ok(());
        }

        let c1 = self.bytes2clusters(node.size)?;
        let c2 = self.bytes2clusters(size)?;
        match c1.cmp(&c2) {
            std::cmp::Ordering::Less => self.grow_file(nid, c1, c2 - c1)?,
            std::cmp::Ordering::Greater => self.shrink_file(nid, c1, c1 - c2)?,
            std::cmp::Ordering::Equal => (),
        }

        get_mut_node!(self.nmap, &nid).valid_size = if erase {
            self.erase_range(nid, get_node!(self.nmap, &nid).valid_size, size)?;
            size
        } else {
            std::cmp::min(get_node!(self.nmap, &nid).valid_size, size)
        };

        let node = get_mut_node!(self.nmap, &nid);
        node.update_mtime();
        node.size = size;
        node.is_dirty = true;
        Ok(())
    }

    #[must_use]
    pub fn get_free_clusters(&self) -> u32 {
        let mut free_clusters = 0;
        for i in 0..self.cmap.size.try_into().unwrap() {
            if bitmap::bmap_get(&self.cmap.chunk, i) == 0 {
                free_clusters += 1;
            }
        }
        free_clusters
    }

    fn find_used_clusters(&mut self, a: &mut u32, b: &mut u32) -> nix::Result<bool> {
        let end = u32::from_le(self.sb.cluster_count);
        let mut va;
        let mut vb = *b;

        // find first used cluster
        va = vb + 1;
        while va < end {
            let i = match (va - exfatfs::EXFAT_FIRST_DATA_CLUSTER).try_into() {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            };
            if bitmap::bmap_get(&self.cmap.chunk, i) != 0 {
                break;
            }
            va += 1;
        }
        *a = va;
        if va >= end {
            return Ok(false);
        }

        // find last contiguous used cluster
        vb = va;
        while vb < end {
            let i = match (vb - exfatfs::EXFAT_FIRST_DATA_CLUSTER).try_into() {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            };
            if bitmap::bmap_get(&self.cmap.chunk, i) == 0 {
                vb -= 1;
                break;
            }
            vb += 1;
        }
        *b = vb;
        Ok(true)
    }

    pub fn find_used_sectors(&mut self, a: &mut u64, b: &mut u64) -> nix::Result<bool> {
        let (mut ca, mut cb) = if *a == 0 && *b == 0 {
            (
                exfatfs::EXFAT_FIRST_DATA_CLUSTER - 1,
                exfatfs::EXFAT_FIRST_DATA_CLUSTER - 1,
            )
        } else {
            (self.s2c(*a)?, self.s2c(*b)?)
        };
        if !self.find_used_clusters(&mut ca, &mut cb)? {
            return Ok(false);
        }
        if *a != 0 || *b != 0 {
            *a = self.c2s(ca);
        }
        *b = self.c2s(cb) + (self.get_cluster_size() - 1) / self.get_sector_size();
        Ok(true)
    }

    pub fn pread(&mut self, nid: node::Nid, buf: &mut [u8], offset: u64) -> nix::Result<u64> {
        let size = buf.len().try_into().unwrap();
        let node = get_node!(self.nmap, &nid);
        let node_valid_size = node.valid_size; // won't change
        let node_size = node.size; // won't change

        let offset_orig = offset;
        if offset >= node_size || size == 0 {
            return Ok(0);
        }
        if offset + size > node_valid_size {
            let mut bytes = 0;
            if offset < node_valid_size {
                bytes = self.pread(
                    nid,
                    &mut buf[..(node_valid_size - offset).try_into().unwrap()],
                    offset_orig,
                )?;
                if bytes < node_valid_size - offset {
                    return Ok(bytes);
                }
            }
            for i in 0..std::cmp::min(size - bytes, node_size - node_valid_size) {
                buf[usize::try_from(bytes + i).unwrap()] = 0;
            }
            return Ok(std::cmp::min(size, node_size - offset));
        }

        let cluster_size = self.get_cluster_size();
        let mut cluster = self.advance_cluster(nid, (offset / cluster_size).try_into().unwrap())?;
        let mut loffset = offset % cluster_size;
        let mut remainder = std::cmp::min(size, node_size - offset);
        let mut i = 0;

        while remainder > 0 {
            if self.cluster_invalid(cluster) {
                log::error!("invalid cluster {cluster:#x} while reading");
                return Err(nix::errno::Errno::EIO);
            }
            let lsize = std::cmp::min(cluster_size - loffset, remainder);
            let lsize_usize = usize::try_from(lsize).unwrap();
            let buf = &mut buf[i..(i + lsize_usize)];
            if let Err(e) = self.dev.pread(buf, self.c2o(cluster) + loffset) {
                log::error!("failed to read cluster {cluster:#x}");
                return Err(util::error2errno(e));
            }
            i += lsize_usize;
            loffset = 0;
            remainder -= lsize;
            cluster = self.next_cluster(nid, cluster);
        }

        let node = get_mut_node!(self.nmap, &nid);
        if !node.is_directory() && self.ro == 0 && !self.opt.noatime {
            node.update_atime();
        }
        Ok(std::cmp::min(size, node_size - offset) - remainder)
    }

    pub fn pwrite(&mut self, nid: node::Nid, buf: &[u8], offset: u64) -> nix::Result<u64> {
        let size = buf.len().try_into().unwrap();
        if offset > get_node!(self.nmap, &nid).size {
            self.truncate(nid, offset, true)?;
        }
        if offset + size > get_node!(self.nmap, &nid).size {
            self.truncate(nid, offset + size, false)?;
        }
        if size == 0 {
            return Ok(0);
        }

        let cluster_size = self.get_cluster_size();
        let mut cluster = self.advance_cluster(nid, (offset / cluster_size).try_into().unwrap())?;
        let mut loffset = offset % cluster_size;
        let mut remainder = size;
        let mut i = 0;

        while remainder > 0 {
            if self.cluster_invalid(cluster) {
                log::error!("invalid cluster {cluster:#x} while writing");
                return Err(nix::errno::Errno::EIO);
            }
            let lsize = std::cmp::min(cluster_size - loffset, remainder);
            let lsize_usize = usize::try_from(lsize).unwrap();
            let buf = &buf[i..(i + lsize_usize)];
            if let Err(e) = self.dev.pwrite(buf, self.c2o(cluster) + loffset) {
                log::error!("failed to write cluster {cluster:#x}");
                return Err(util::error2errno(e));
            }
            i += lsize_usize;
            loffset = 0;
            remainder -= lsize;
            let node = get_mut_node!(self.nmap, &nid);
            node.valid_size = std::cmp::max(node.valid_size, offset + size - remainder);
            cluster = self.next_cluster(nid, cluster);
        }

        let node = get_mut_node!(self.nmap, &nid);
        if !node.is_directory() {
            // directory's mtime should be updated by the caller only when it
            // creates or removes something in this directory
            node.update_mtime();
        }
        Ok(size - remainder)
    }

    fn read_entries(
        &mut self,
        dnid: node::Nid,
        n: usize,
        offset: u64,
    ) -> nix::Result<Vec<exfatfs::ExfatEntry>> {
        assert_ne!(n, 0);
        assert!(
            get_node!(self.nmap, &dnid).is_directory(),
            "attempted to read entries from a file"
        );
        let mut entries = exfatfs::ExfatEntry::bulk_new(n);
        let buf_size = exfatfs::EXFAT_ENTRY_SIZE * n;
        let mut buf = vec![0; buf_size];
        let size = self.pread(dnid, &mut buf, offset)?;
        if size == buf_size.try_into().unwrap() {
            for (i, entry) in entries.iter_mut().enumerate() {
                let beg = exfatfs::EXFAT_ENTRY_SIZE * i;
                let end = beg + exfatfs::EXFAT_ENTRY_SIZE;
                let (prefix, body, suffix) =
                    unsafe { buf[beg..end].align_to::<exfatfs::ExfatEntry>() };
                assert!(prefix.is_empty());
                assert!(suffix.is_empty());
                *entry = body[0]; // extra copy
            }
            return Ok(entries); // success
        }
        if size == 0 {
            return Err(nix::errno::Errno::ENOENT);
        }
        log::error!("read {size} bytes instead of {buf_size} bytes");
        Err(nix::errno::Errno::EIO)
    }

    fn write_entries(
        &mut self,
        dnid: node::Nid,
        entries: &[exfatfs::ExfatEntry],
        n: usize,
        offset: u64,
    ) -> nix::Result<()> {
        assert_ne!(n, 0);
        assert!(
            get_node!(self.nmap, &dnid).is_directory(),
            "attempted to write entries into a file"
        );
        let mut buf = vec![];
        for entry in entries.iter().take(n) {
            buf.extend_from_slice(unsafe { util::any_as_u8_slice(entry) }); // extra copy
        }
        let buf_size = exfatfs::EXFAT_ENTRY_SIZE * n;
        assert_eq!(buf.len(), buf_size);
        let size = self.pwrite(dnid, &buf, offset)?;
        if size == buf_size.try_into().unwrap() {
            return Ok(()); // success
        }
        log::error!("wrote {size} bytes instead of {buf_size} bytes");
        Err(nix::errno::Errno::EIO)
    }

    fn check_entries(&mut self, entry: &[exfatfs::ExfatEntry], n: usize) -> bool {
        const EXFAT_ENTRY_FILE_I32: i32 = exfatfs::EXFAT_ENTRY_FILE as i32;
        const EXFAT_ENTRY_FILE_INFO_I32: i32 = exfatfs::EXFAT_ENTRY_FILE_INFO as i32;
        const EXFAT_ENTRY_FILE_NAME_I32: i32 = exfatfs::EXFAT_ENTRY_FILE_NAME as i32;
        const EXFAT_ENTRY_FILE_TAIL_I32: i32 = exfatfs::EXFAT_ENTRY_FILE_TAIL as i32;

        const EXFAT_ENTRY_MAX: u8 = 0xff;
        const EXFAT_ENTRY_MAX_I32: i32 = EXFAT_ENTRY_MAX as i32;
        const EXFAT_ENTRY_VOID: i32 = -1;

        let mut previous = EXFAT_ENTRY_VOID;
        let mut current;
        // check transitions between entries types
        for (i, x) in entry.iter().enumerate().take(n + 1) {
            current = if i < n { x.typ } else { EXFAT_ENTRY_MAX };
            let valid = match previous {
                EXFAT_ENTRY_VOID => current == exfatfs::EXFAT_ENTRY_FILE,
                EXFAT_ENTRY_FILE_I32 => current == exfatfs::EXFAT_ENTRY_FILE_INFO,
                EXFAT_ENTRY_FILE_INFO_I32 => current == exfatfs::EXFAT_ENTRY_FILE_NAME,
                EXFAT_ENTRY_FILE_NAME_I32 => {
                    if current == exfatfs::EXFAT_ENTRY_FILE_NAME || current == EXFAT_ENTRY_MAX {
                        true
                    } else {
                        current >= exfatfs::EXFAT_ENTRY_FILE_TAIL
                    }
                }
                EXFAT_ENTRY_FILE_TAIL_I32..=EXFAT_ENTRY_MAX_I32 => {
                    if current == EXFAT_ENTRY_MAX {
                        true
                    } else {
                        current >= exfatfs::EXFAT_ENTRY_FILE_TAIL
                    }
                }
                _ => false,
            };
            if !valid {
                for x in entry {
                    log::error!("{x:?}");
                }
                error_or_panic!(
                    "unexpected entry type {current:#x} after {previous:#x} at {i}/{n}",
                    self.opt.debug
                );
            }
            previous = current.into();
        }
        true
    }

    fn check_node(
        &mut self,
        nid: node::Nid,
        actual_checksum: u16,
        meta1: &exfatfs::ExfatEntryMeta1,
    ) -> bool {
        let mut ret = true;
        // Validate checksum first. If it's invalid all other fields probably
        // contain just garbage.
        if u16::from_le(actual_checksum) != u16::from_le(meta1.checksum) {
            log::error!(
                "'{}' has invalid checksum ({:#x} != {:#x})",
                get_node!(self.nmap, &nid).get_name(),
                u16::from_le(actual_checksum),
                u16::from_le(meta1.checksum)
            );
            if !(self.ask_to_fix() && self.fix_invalid_node_checksum(nid)) {
                ret = false;
            }
        }

        // exFAT does not support sparse files but allows files with uninitialized
        // clusters. For such files valid_size means initialized data size and
        // cannot be greater than file size. See SetFileValidData() function
        // description in MSDN.
        let node = get_node!(self.nmap, &nid);
        if node.valid_size > node.size {
            log::error!(
                "'{}' has valid size ({}) greater than size ({})",
                node.get_name(),
                node.valid_size,
                node.size
            );
            ret = false;
        }

        // Empty file must have zero start cluster. Non-empty file must start
        // with a valid cluster. Directories cannot be empty (i.e. must always
        // have a valid start cluster), but we will check this later while
        // reading that directory to give user a chance to read this directory.
        let node = get_node!(self.nmap, &nid);
        if node.size == 0 && node.start_cluster != exfatfs::EXFAT_CLUSTER_FREE {
            log::error!(
                "'{}' is empty but start cluster is {:#x}",
                node.get_name(),
                node.start_cluster
            );
            ret = false;
        }
        let node = get_node!(self.nmap, &nid);
        if node.size > 0 && self.cluster_invalid(node.start_cluster) {
            log::error!(
                "'{}' points to invalid cluster {:#x}",
                node.get_name(),
                node.start_cluster
            );
            ret = false;
        }

        // File or directory cannot be larger than clusters heap.
        let node = get_node!(self.nmap, &nid);
        let clusters_heap_size =
            u64::from(u32::from_le(self.sb.cluster_count)) * self.get_cluster_size();
        if node.size > clusters_heap_size {
            log::error!(
                "'{}' is larger than clusters heap: {} > {}",
                node.get_name(),
                node.size,
                clusters_heap_size
            );
            ret = false;
        }

        // Empty file or directory must be marked as non-contiguous.
        let node = get_node!(self.nmap, &nid);
        if node.size == 0 && node.is_contiguous {
            log::error!(
                "'{}' is empty but marked as contiguous ({:#x})",
                node.get_name(),
                node.attrib
            );
            ret = false;
        }

        // Directory size must be aligned on at cluster boundary.
        let node = get_node!(self.nmap, &nid);
        if node.is_directory() && (node.size % self.get_cluster_size()) != 0 {
            log::error!(
                "'{}' directory size {} is not divisible by {}",
                node.get_name(),
                node.size,
                self.get_cluster_size()
            );
            ret = false;
        }
        ret
    }

    fn parse_file_entries(
        &mut self,
        dnid: node::Nid,
        entries: &[exfatfs::ExfatEntry],
        n: usize,
        offset: u64,
    ) -> nix::Result<node::Nid> {
        if !self.check_entries(entries, n) {
            return Err(nix::errno::Errno::EIO);
        }

        let meta1: &exfatfs::ExfatEntryMeta1 = bytemuck::cast_ref(&entries[0]);
        if meta1.continuations < 2 {
            log::error!("too few continuations ({})", meta1.continuations);
            return Err(nix::errno::Errno::EIO);
        }

        let meta2: &exfatfs::ExfatEntryMeta2 = bytemuck::cast_ref(&entries[1]);
        if (meta2.flags & !(exfatfs::EXFAT_FLAG_ALWAYS1 | exfatfs::EXFAT_FLAG_CONTIGUOUS)) != 0 {
            log::error!("unknown flags in meta2 ({:#x})", meta2.flags);
            return Err(nix::errno::Errno::EIO);
        }

        let mandatory_entries = 2 + util::div_round_up!(
            meta2.name_length,
            u8::try_from(exfatfs::EXFAT_ENAME_MAX).unwrap()
        );
        if meta1.continuations < mandatory_entries - 1 {
            log::error!(
                "too few continuations ({} < {})",
                meta1.continuations,
                mandatory_entries - 1
            );
            return Err(nix::errno::Errno::EIO);
        }

        let mut node = self.alloc_node();
        node.entry_offset = offset;
        node.init_meta1(meta1);
        node.init_meta2(meta2);
        node.init_name(&entries[2..], usize::from(mandatory_entries) - 2);
        assert!(node.is_valid());
        let nid = node.nid;
        self.nmap_attach(dnid, node);

        if !self.check_node(nid, util::calc_checksum(entries, n), meta1) {
            return Err(nix::errno::Errno::EIO);
        }
        Ok(nid)
    }

    fn parse_file_entry(
        &mut self,
        dnid: node::Nid,
        offset: u64,
        n: usize,
    ) -> nix::Result<(node::Nid, u64)> {
        let entries = self.read_entries(dnid, n, offset)?;
        Ok((
            self.parse_file_entries(dnid, &entries, n, offset)?,
            offset + exfatfs::EXFAT_ENTRY_SIZE_U64 * u64::try_from(n).unwrap(),
        ))
    }

    fn decompress_upcase(output: &mut [u16], source: &[u16], size: usize) {
        for (oi, x) in output
            .iter_mut()
            .enumerate()
            .take(exfatfs::EXFAT_UPCASE_CHARS)
        {
            *x = oi.try_into().unwrap();
        }
        let mut si = 0;
        let mut oi = 0;
        while si < size && oi < exfatfs::EXFAT_UPCASE_CHARS {
            let ch = u16::from_le(source[si]);
            if ch == 0xffff && si + 1 < size {
                // indicates a run
                si += 1;
                oi += usize::from(u16::from_le(source[si]));
            } else {
                output[oi] = ch;
                oi += 1;
            }
            si += 1;
        }
    }

    // Read one entry in a directory at offset position and build a new node
    // structure.
    fn readdir(&mut self, dnid: node::Nid, offset: u64) -> nix::Result<(node::Nid, u64)> {
        let mut offset = offset;
        loop {
            let entry = &self.read_entries(dnid, 1, offset)?[0];
            match entry.typ {
                exfatfs::EXFAT_ENTRY_FILE => {
                    let meta1: &exfatfs::ExfatEntryMeta1 = bytemuck::cast_ref(entry);
                    return self.parse_file_entry(
                        dnid,
                        offset,
                        usize::from(1 + meta1.continuations),
                    );
                }
                exfatfs::EXFAT_ENTRY_UPCASE => 'upcase_label: {
                    if !self.upcase.is_empty() {
                        break 'upcase_label;
                    }
                    let upcase: &exfatfs::ExfatEntryUpcase = bytemuck::cast_ref(entry);
                    self.readdir_entry_upcase(upcase)?;
                }
                exfatfs::EXFAT_ENTRY_BITMAP => {
                    let bitmap: &exfatfs::ExfatEntryBitmap = bytemuck::cast_ref(entry);
                    self.readdir_entry_bitmap(bitmap)?;
                }
                exfatfs::EXFAT_ENTRY_LABEL => {
                    let label: &exfatfs::ExfatEntryLabel = bytemuck::cast_ref(entry);
                    self.readdir_entry_label(label)?;
                }
                _ => 'default_label: {
                    if entry.typ & exfatfs::EXFAT_ENTRY_VALID == 0 {
                        break 'default_label; // deleted entry, ignore it
                    }
                    log::error!("unknown entry type {:#x}", entry.typ);
                    if !self.ask_to_fix() {
                        return Err(nix::errno::Errno::ECANCELED);
                    }
                    self.fix_unknown_entry(dnid, entry, offset)?;
                }
            }
            offset += exfatfs::EXFAT_ENTRY_SIZE_U64;
        }
        // we never reach here
    }

    fn readdir_entry_upcase(&mut self, upcase: &exfatfs::ExfatEntryUpcase) -> nix::Result<()> {
        if self.cluster_invalid(u32::from_le(upcase.start_cluster)) {
            log::error!(
                "invalid cluster {:#x} in upcase table",
                u32::from_le(upcase.start_cluster)
            );
            return Err(nix::errno::Errno::EIO);
        }
        let upcase_size = u64::from_le(upcase.size);
        let upcase_size_usize = usize::try_from(upcase_size).unwrap();
        if upcase_size == 0
            || upcase_size_usize > exfatfs::EXFAT_UPCASE_CHARS * std::mem::size_of::<u16>()
            || upcase_size_usize % std::mem::size_of::<u16>() != 0
        {
            log::error!("bad upcase table size ({upcase_size} bytes)");
            return Err(nix::errno::Errno::EIO);
        }

        // read compressed upcase table
        let buf = match self
            .dev
            .preadx(upcase_size, self.c2o(u32::from_le(upcase.start_cluster)))
        {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "failed to read upper case table ({} bytes starting at cluster {:#x})",
                    upcase_size,
                    u32::from_le(upcase.start_cluster)
                );
                return Err(util::error2errno(e));
            }
        };

        // decompress upcase table
        let mut upcase_comp = vec![0; upcase_size_usize / std::mem::size_of::<u16>()];
        // relan/exfat implicitly assumes le
        byteorder::LittleEndian::read_u16_into(&buf, &mut upcase_comp);
        self.upcase = vec![0; exfatfs::EXFAT_UPCASE_CHARS];
        Self::decompress_upcase(
            &mut self.upcase,
            &upcase_comp,
            upcase_size_usize / std::mem::size_of::<u16>(),
        );
        Ok(())
    }

    fn readdir_entry_bitmap(&mut self, bitmap: &exfatfs::ExfatEntryBitmap) -> nix::Result<()> {
        self.cmap.start_cluster = u32::from_le(bitmap.start_cluster);
        if self.cluster_invalid(self.cmap.start_cluster) {
            log::error!(
                "invalid cluster {:#x} in clusters bitmap",
                self.cmap.start_cluster
            );
            return Err(nix::errno::Errno::EIO);
        }

        self.cmap.size = u32::from_le(self.sb.cluster_count);
        if u64::from_le(bitmap.size) < util::div_round_up!(u64::from(self.cmap.size), 8) {
            log::error!(
                "invalid clusters bitmap size: {} (expected at least {})",
                u64::from_le(bitmap.size),
                util::div_round_up!(self.cmap.size, 8)
            );
            return Err(nix::errno::Errno::EIO);
        }
        // bitmap can be rather big, up to 512 MB
        self.cmap.chunk_size = self.cmap.size;

        let buf_size = bitmap::bmap_size(self.cmap.chunk_size.try_into().unwrap())
            .try_into()
            .unwrap();
        // XXX works only when bitmap::Bitmap is u8
        self.cmap.chunk = match self.dev.preadx(buf_size, self.c2o(self.cmap.start_cluster)) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "failed to read clusters bitmap ({} bytes starting at cluster {:#x})",
                    u64::from_le(bitmap.size),
                    self.cmap.start_cluster
                );
                return Err(util::error2errno(e));
            }
        };
        Ok(())
    }

    fn readdir_entry_label(&mut self, label: &exfatfs::ExfatEntryLabel) -> nix::Result<()> {
        if usize::from(label.length) > exfatfs::EXFAT_ENAME_MAX {
            log::error!("too long label ({} chars)", label.length);
            return Err(nix::errno::Errno::EIO);
        }
        let output = utf::utf16_to_utf8(
            &label.name,
            EXFAT_UTF8_ENAME_BUFFER_MAX,
            exfatfs::EXFAT_ENAME_MAX,
        )?;
        self.init_strlabel(&output);
        Ok(())
    }

    fn cache_directory(&mut self, dnid: node::Nid) -> nix::Result<()> {
        if get_node!(self.nmap, &dnid).is_cached {
            return Ok(()); // already cached
        }
        let mut nids = vec![];
        let mut offset = 0;
        loop {
            let (nid, next) = match self.readdir(dnid, offset) {
                Ok(v) => v,
                Err(nix::errno::Errno::ENOENT) => break,
                Err(e) => {
                    // relan/exfat rollbacks all nodes in this directory
                    // (not just the ones added now)
                    for nid in &nids {
                        self.nmap_detach(dnid, *nid);
                    }
                    return Err(e);
                }
            };
            if nid != node::NID_INVALID {
                nids.push(nid);
            }
            offset = next;
        }
        get_mut_node!(self.nmap, &dnid).is_cached = true;
        Ok(())
    }

    fn nmap_attach(&mut self, dnid: node::Nid, mut node: node::ExfatNode) {
        assert_ne!(dnid, node::NID_INVALID);
        assert_ne!(node.nid, node::NID_INVALID);
        assert_ne!(node.nid, node::NID_ROOT); // root directly uses nmap
        let dnode = get_mut_node!(self.nmap, &dnid);
        node.pnid = dnode.nid;
        dnode.cnids.push(node.nid);
        assert!(self.nmap.insert(node.nid, node).is_none());
    }

    fn nmap_detach(&mut self, dnid: node::Nid, nid: node::Nid) -> node::ExfatNode {
        assert_ne!(dnid, node::NID_INVALID);
        assert_ne!(nid, node::NID_INVALID);
        assert_ne!(nid, node::NID_ROOT); // root directly uses nmap
        let dnode = get_mut_node!(self.nmap, &dnid);
        if let Some(i) = dnode.cnids.iter().position(|x| *x == nid) {
            dnode.cnids.swap_remove(i);
        }
        let mut node = self.nmap.remove(&nid).unwrap();
        node.pnid = node::NID_INVALID; // sanity
        node
    }

    fn reset_cache_impl(&mut self, nid: node::Nid) {
        while !get_node!(self.nmap, &nid).cnids.is_empty() {
            let cnid = get_node!(self.nmap, &nid).cnids[0];
            self.reset_cache_impl(cnid);
            self.nmap_detach(nid, cnid);
        }
        let node = get_mut_node!(self.nmap, &nid);
        node.is_cached = false;
        assert_eq!(
            node.references,
            0,
            "non-zero reference counter ({}) for '{}'",
            node.references,
            node.get_name()
        ); // exfat_warn() in relan/exfat
        assert!(
            node.nid == node::NID_ROOT || !node.is_dirty,
            "node '{}' is dirty",
            node.get_name()
        );
        while node.references > 0 {
            node.put();
        }
    }

    fn reset_cache(&mut self) {
        self.reset_cache_impl(node::NID_ROOT);
    }

    pub fn flush_node(&mut self, nid: node::Nid) -> nix::Result<()> {
        let node = get_node!(self.nmap, &nid);
        if !node.is_dirty {
            return Ok(()); // no need to flush
        }
        assert_eq!(self.ro, 0, "unable to flush node to read-only FS");
        if node.pnid == node::NID_INVALID {
            return Ok(()); // do not flush unlinked node
        }

        let mut entries = self.read_entries(
            node.pnid,
            (1 + node.continuations).into(),
            node.entry_offset,
        )?;
        let node = get_node!(self.nmap, &nid);
        if !self.check_entries(&entries, (1 + node.continuations).into()) {
            return Err(nix::errno::Errno::EIO);
        }

        let node = get_node!(self.nmap, &nid);
        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.attrib = node.attrib.to_le();
        let (date, time, centisec, tzoffset) = time::unix2exfat(node.mtime);
        meta1.mdate = date;
        meta1.mtime = time;
        meta1.mtime_cs = centisec;
        meta1.mtime_tzo = tzoffset;
        let (date, time, _, tzoffset) = time::unix2exfat(node.atime);
        meta1.adate = date;
        meta1.atime = time;
        meta1.atime_tzo = tzoffset;

        let meta2: &mut exfatfs::ExfatEntryMeta2 = bytemuck::cast_mut(&mut entries[1]);
        meta2.valid_size = node.valid_size.to_le();
        meta2.size = node.size.to_le();
        meta2.start_cluster = node.start_cluster.to_le();
        meta2.flags = exfatfs::EXFAT_FLAG_ALWAYS1;
        // empty files must not be marked as contiguous
        if node.size != 0 && node.is_contiguous {
            meta2.flags |= exfatfs::EXFAT_FLAG_CONTIGUOUS;
        }
        // name hash remains unchanged, no need to recalculate it

        let checksum = util::calc_checksum(&entries, (1 + node.continuations).into());
        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.checksum = checksum;
        self.write_entries(
            node.pnid,
            &entries,
            (1 + node.continuations).into(),
            node.entry_offset,
        )?;
        get_mut_node!(self.nmap, &nid).is_dirty = false;
        self.flush()
    }

    fn erase_entries(&mut self, dnid: node::Nid, n: usize, offset: u64) -> nix::Result<()> {
        let mut entries = self.read_entries(dnid, n, offset)?;
        for entry in &mut entries {
            entry.typ &= !exfatfs::EXFAT_ENTRY_VALID;
        }
        self.write_entries(dnid, &entries, n, offset)
    }

    fn erase_node(&mut self, nid: node::Nid) -> nix::Result<()> {
        let node = get_node!(self.nmap, &nid);
        let dnid = node.pnid;
        let node_continuations = node.continuations;
        let node_entry_offset = node.entry_offset;
        get_mut_node!(self.nmap, &dnid).get();
        if let Err(e) = self.erase_entries(dnid, (1 + node_continuations).into(), node_entry_offset)
        {
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }
        let result = self.flush_node(dnid);
        get_mut_node!(self.nmap, &dnid).put();
        result
    }

    fn shrink_directory(&mut self, dnid: node::Nid, deleted_offset: u64) -> nix::Result<()> {
        let dnode = get_node!(self.nmap, &dnid);
        assert!(dnode.is_directory(), "attempted to shrink a file");
        assert!(dnode.is_cached, "attempted to shrink uncached directory");

        let mut last_nid = node::NID_INVALID;
        if !dnode.cnids.is_empty() {
            last_nid = dnode.cnids[0];
            for cnid in &dnode.cnids {
                let node = get_node!(self.nmap, cnid);
                if deleted_offset < node.entry_offset {
                    // there are other entries after the removed one, no way to shrink
                    // this directory
                    return Ok(());
                }
                if get_node!(self.nmap, &last_nid).entry_offset < node.entry_offset {
                    last_nid = node.nid;
                }
            }
        }

        let mut entries = 0;
        if last_nid != node::NID_INVALID {
            let last_node = get_node!(self.nmap, &last_nid);
            // offset of the last entry
            entries += last_node.entry_offset / exfatfs::EXFAT_ENTRY_SIZE_U64;
            // two subentries with meta info
            entries += 2;
            // subentries with file name
            entries += u64::try_from(util::div_round_up!(
                utf::utf16_length(&last_node.name),
                exfatfs::EXFAT_ENAME_MAX
            ))
            .unwrap();
        }

        let mut new_size = util::div_round_up!(
            entries * exfatfs::EXFAT_ENTRY_SIZE_U64,
            self.get_cluster_size()
        ) * self.get_cluster_size();
        if new_size == 0 {
            // directory always has at least 1 cluster
            new_size = self.get_cluster_size();
        }
        if new_size == dnode.size {
            return Ok(());
        }
        self.truncate(dnid, new_size, true)
    }

    fn delete(&mut self, nid: node::Nid) -> nix::Result<()> {
        // erase node entry from parent directory
        let dnid = get_node!(self.nmap, &nid).pnid;
        get_mut_node!(self.nmap, &dnid).get();
        if let Err(e) = self.erase_node(nid) {
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }

        // free all clusters and node structure itself
        if let Err(e) = self.truncate(nid, 0, true) {
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }
        // ^^^ relan/exfat keeps clusters until freeing node pointer,
        // but node is gone after detach in Rust.

        let deleted_offset = get_node!(self.nmap, &nid).entry_offset;
        // detach node before shrink_directory()
        let mut node = self.nmap_detach(dnid, nid);
        assert!(node.references > 0);
        // can't undirty truncated node via flush_node() after erase
        node.is_dirty = false;
        // relan/exfat requires caller to put() between delete and truncate
        node.put();
        assert_eq!(node.references, 0); // node is done

        // shrink parent directory
        if let Err(e) = self.shrink_directory(dnid, deleted_offset) {
            if let Err(e) = self.flush_node(dnid) {
                log::error!("{e}");
            }
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }

        // flush parent directory
        get_mut_node!(self.nmap, &dnid).update_mtime();
        let result = self.flush_node(dnid);
        get_mut_node!(self.nmap, &dnid).put();
        result
    }

    pub fn unlink(&mut self, nid: node::Nid) -> nix::Result<()> {
        let node = get_node!(self.nmap, &nid);
        if node.references > 1 {
            return Err(nix::errno::Errno::EBUSY); // XXX open-unlink unsupported
        }
        if node.is_directory() {
            return Err(nix::errno::Errno::EISDIR);
        }
        self.delete(nid)
    }

    pub fn rmdir(&mut self, nid: node::Nid) -> nix::Result<()> {
        let node = get_node!(self.nmap, &nid);
        if node.references > 1 {
            return Err(nix::errno::Errno::EBUSY); // XXX open-unlink unsupported
        }
        if !node.is_directory() {
            return Err(nix::errno::Errno::ENOTDIR);
        }
        // check that directory is empty
        self.cache_directory(nid)?; // populate cnids
        if !get_node!(self.nmap, &nid).cnids.is_empty() {
            return Err(nix::errno::Errno::ENOTEMPTY);
        }
        self.delete(nid)
    }

    fn check_slot(&mut self, dnid: node::Nid, offset: u64, n: usize) -> nix::Result<()> {
        // Root directory contains entries, that don't have any nodes associated
        // with them (clusters bitmap, upper case table, label). We need to be
        // careful not to overwrite them.
        if dnid != node::NID_ROOT {
            return Ok(());
        }
        let entries = self.read_entries(dnid, n, offset)?;
        for entry in &entries {
            if entry.typ & exfatfs::EXFAT_ENTRY_VALID != 0 {
                return Err(nix::errno::Errno::EINVAL);
            }
        }
        Ok(())
    }

    fn find_slot(&mut self, dnid: node::Nid, n: usize) -> nix::Result<u64> {
        let dnode = get_node!(self.nmap, &dnid);
        assert!(dnode.is_cached, "directory is not cached");

        // build a bitmap of valid entries in the directory
        // relan/exfat: why calloc(..., sizeof(bitmap_t)) ?
        let nentries = usize::try_from(dnode.size).unwrap() / exfatfs::EXFAT_ENTRY_SIZE;
        let mut dmap = bitmap::bmap_alloc(nentries);
        for cnid in &dnode.cnids {
            let node = get_node!(self.nmap, cnid);
            for i in 0..=node.continuations {
                bitmap::bmap_set(
                    &mut dmap,
                    usize::try_from(node.entry_offset).unwrap() / exfatfs::EXFAT_ENTRY_SIZE
                        + usize::from(i),
                );
            }
        }

        // find a slot in the directory entries bitmap
        let mut offset = 0;
        let mut contiguous = 0;
        let mut i = 0;
        while i < nentries {
            if bitmap::bmap_get(&dmap, i) == 0 {
                if contiguous == 0 {
                    offset = u64::try_from(i).unwrap() * exfatfs::EXFAT_ENTRY_SIZE_U64;
                }
                contiguous += 1;
                if contiguous == n {
                    // suitable slot is found, check that it's not occupied
                    match self.check_slot(dnid, offset, n) {
                        Ok(()) => return Ok(offset), // slot is free
                        Err(nix::errno::Errno::EINVAL) => {
                            // slot at (i-n) is occupied, go back and check (i-n+1)
                            i -= contiguous - 1;
                            contiguous = 0;
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                contiguous = 0;
            }
            i += 1;
        }

        // no suitable slots found, extend the directory
        let dir_size = get_node!(self.nmap, &dnid).size;
        if contiguous == 0 {
            offset = dir_size;
        }
        self.truncate(
            dnid,
            util::round_up!(
                dir_size + exfatfs::EXFAT_ENTRY_SIZE_U64 * u64::try_from(n - contiguous).unwrap(),
                self.get_cluster_size()
            ),
            true,
        )?;
        Ok(offset)
    }

    fn commit_entry(
        &mut self,
        dnid: node::Nid,
        name: &[u16],
        offset: u64,
        attrib: u16,
    ) -> nix::Result<node::Nid> {
        let name_length = utf::utf16_length(name);
        let name_entries = util::div_round_up!(name_length, exfatfs::EXFAT_ENAME_MAX);
        let mut entries = exfatfs::ExfatEntry::bulk_new(2 + name_entries);

        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.typ = exfatfs::EXFAT_ENTRY_FILE;
        meta1.continuations = 1 + u8::try_from(name_entries).unwrap();
        meta1.attrib = attrib.to_le();
        let (date, time, centisec, tzoffset) = time::unix2exfat(util::get_current_time());
        meta1.adate = date;
        meta1.mdate = date;
        meta1.crdate = date;
        meta1.atime = time;
        meta1.mtime = time;
        meta1.crtime = time;
        meta1.mtime_cs = centisec; // there is no atime_cs
        meta1.crtime_cs = centisec;
        meta1.atime_tzo = tzoffset;
        meta1.mtime_tzo = tzoffset;
        meta1.crtime_tzo = tzoffset;

        let meta2: &mut exfatfs::ExfatEntryMeta2 = bytemuck::cast_mut(&mut entries[1]);
        meta2.typ = exfatfs::EXFAT_ENTRY_FILE_INFO;
        meta2.flags = exfatfs::EXFAT_FLAG_ALWAYS1;
        meta2.name_length = name_length.try_into().unwrap();
        meta2.name_hash = util::calc_name_hash(&self.upcase, name, name_length);
        meta2.start_cluster = exfatfs::EXFAT_CLUSTER_FREE.to_le();

        for i in 0..name_entries {
            let name_entry: &mut exfatfs::ExfatEntryName = bytemuck::cast_mut(&mut entries[2 + i]);
            name_entry.typ = exfatfs::EXFAT_ENTRY_FILE_NAME;
            name_entry.unknown = 0;
            let name = &name[(i * exfatfs::EXFAT_ENAME_MAX)..];
            name_entry
                .name
                .copy_from_slice(&name[..exfatfs::EXFAT_ENAME_MAX]);
        }

        let checksum = util::calc_checksum(&entries, 2 + name_entries);
        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.checksum = checksum;
        self.write_entries(dnid, &entries, 2 + name_entries, offset)?;

        let mut node = self.alloc_node();
        node.entry_offset = offset;
        node.init_meta1(bytemuck::cast_ref(&entries[0]));
        node.init_meta2(bytemuck::cast_ref(&entries[1]));
        node.init_name(&entries[2..], name_entries);
        assert!(node.is_valid());
        let nid = node.nid;
        self.nmap_attach(dnid, node);
        Ok(nid)
    }

    fn create_at(&mut self, dnid: node::Nid, path: &str, attrib: u16) -> nix::Result<node::Nid> {
        let (dnid, enid, name) = self.split_at(dnid, path)?;
        if enid != node::NID_INVALID {
            get_mut_node!(self.nmap, &enid).put();
            get_mut_node!(self.nmap, &dnid).put();
            return Err(nix::errno::Errno::EEXIST);
        }
        let offset = match self.find_slot(
            dnid,
            2 + util::div_round_up!(utf::utf16_length(&name), exfatfs::EXFAT_ENAME_MAX),
        ) {
            Ok(v) => v,
            Err(e) => {
                get_mut_node!(self.nmap, &dnid).put();
                return Err(e);
            }
        };
        let nid = match self.commit_entry(dnid, &name, offset, attrib) {
            Ok(v) => v,
            Err(e) => {
                get_mut_node!(self.nmap, &dnid).put();
                return Err(e);
            }
        };
        get_mut_node!(self.nmap, &dnid).update_mtime();
        if let Err(e) = self.flush_node(dnid) {
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }
        get_mut_node!(self.nmap, &dnid).put();
        Ok(nid)
    }

    pub fn mknod(&mut self, path: &str) -> nix::Result<node::Nid> {
        self.mknod_at(node::NID_ROOT, path)
    }

    pub fn mknod_at(&mut self, dnid: node::Nid, path: &str) -> nix::Result<node::Nid> {
        let nid = self.create_at(dnid, path, exfatfs::EXFAT_ATTRIB_ARCH)?;
        if self.opt.debug {
            assert_eq!(nid, self.lookup_at(dnid, path)?);
            get_mut_node!(self.nmap, &nid).put();
        }
        Ok(nid)
    }

    pub fn mkdir(&mut self, path: &str) -> nix::Result<node::Nid> {
        self.mkdir_at(node::NID_ROOT, path)
    }

    pub fn mkdir_at(&mut self, dnid: node::Nid, path: &str) -> nix::Result<node::Nid> {
        let nid = self.create_at(dnid, path, exfatfs::EXFAT_ATTRIB_DIR)?;
        // relan/exfat unconditionally lookup the path for node
        if self.opt.debug {
            // relan/exfat returns 0 on lookup failure
            assert_eq!(nid, self.lookup_at(dnid, path)?);
            get_mut_node!(self.nmap, &nid).put();
        }
        get_mut_node!(self.nmap, &nid).get();
        // directories always have at least one cluster
        if let Err(e) = self.truncate(nid, self.get_cluster_size(), true) {
            if let Err(e) = self.delete(nid) {
                log::error!("{e}");
            }
            get_mut_node!(self.nmap, &nid).put();
            return Err(e);
        }
        if let Err(e) = self.flush_node(nid) {
            if let Err(e) = self.delete(nid) {
                log::error!("{e}");
            }
            get_mut_node!(self.nmap, &nid).put();
            return Err(e);
        }
        get_mut_node!(self.nmap, &nid).put();
        Ok(nid)
    }

    fn rename_entry(
        &mut self,
        old_dnid: node::Nid,
        new_dnid: node::Nid,
        nid: node::Nid,
        name: &[u16],
        new_offset: u64,
    ) -> nix::Result<()> {
        let name_length = utf::utf16_length(name);
        let name_entries = util::div_round_up!(name_length, exfatfs::EXFAT_ENAME_MAX);

        let node = get_node!(self.nmap, &nid);
        let mut entries = self.read_entries(node.pnid, 2, node.entry_offset)?;
        let v = exfatfs::ExfatEntry::bulk_new(name_entries);
        entries.extend_from_slice(&v);
        assert_eq!(entries.len(), 2 + name_entries);

        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.continuations = 1 + u8::try_from(name_entries).unwrap();

        let meta2: &mut exfatfs::ExfatEntryMeta2 = bytemuck::cast_mut(&mut entries[1]);
        meta2.name_length = name_length.try_into().unwrap();
        meta2.name_hash = util::calc_name_hash(&self.upcase, name, name_length);

        self.erase_node(nid)?;
        let node = get_mut_node!(self.nmap, &nid);
        node.entry_offset = new_offset;
        node.continuations = 1 + u8::try_from(name_entries).unwrap();

        for i in 0..name_entries {
            let name_entry: &mut exfatfs::ExfatEntryName = bytemuck::cast_mut(&mut entries[2 + i]);
            name_entry.typ = exfatfs::EXFAT_ENTRY_FILE_NAME;
            name_entry.unknown = 0;
            let name = &name[(i * exfatfs::EXFAT_ENAME_MAX)..];
            name_entry
                .name
                .copy_from_slice(&name[..exfatfs::EXFAT_ENAME_MAX]);
        }

        let checksum = util::calc_checksum(&entries, 2 + name_entries);
        let meta1: &mut exfatfs::ExfatEntryMeta1 = bytemuck::cast_mut(&mut entries[0]);
        meta1.checksum = checksum;
        self.write_entries(new_dnid, &entries, 2 + name_entries, new_offset)?;

        let node = get_mut_node!(self.nmap, &nid);
        node.update_name(&entries[2..], name_entries);
        assert!(node.is_valid());

        // update pnid / cnids to move nid from old_dnid to new_dnid
        let node = self.nmap_detach(old_dnid, nid);
        self.nmap_attach(new_dnid, node);
        Ok(())
    }

    pub fn rename(&mut self, old_path: &str, new_path: &str) -> nix::Result<node::Nid> {
        self.rename_at(node::NID_ROOT, old_path, node::NID_ROOT, new_path)
    }

    pub fn rename_at(
        &mut self,
        old_dnid: node::Nid,
        old_path: &str,
        new_dnid: node::Nid,
        new_path: &str,
    ) -> nix::Result<node::Nid> {
        let nid = self.lookup_at(old_dnid, old_path)?;
        let (dnid, enid, name) = match self.split_at(new_dnid, new_path) {
            Ok(v) => v,
            Err(e) => {
                get_mut_node!(self.nmap, &nid).put();
                return Err(e);
            }
        };

        // check that target is not a subdirectory of the source
        if get_node!(self.nmap, &nid).is_directory() {
            let mut dnid = dnid;
            loop {
                if nid == dnid {
                    if enid != node::NID_INVALID {
                        get_mut_node!(self.nmap, &enid).put();
                    }
                    get_mut_node!(self.nmap, &dnid).put();
                    get_mut_node!(self.nmap, &nid).put();
                    return Err(nix::errno::Errno::EINVAL);
                }
                dnid = get_node!(self.nmap, &dnid).pnid;
                if dnid == node::NID_INVALID {
                    break;
                }
            }
        }

        if enid != node::NID_INVALID {
            // remove target if it's not the same node as source
            if enid != nid {
                // unlink_rename_target puts enid regardless of result
                if let Err(e) = self.unlink_rename_target(enid, nid) {
                    // free clusters even if something went wrong; otherwise they
                    // will be just lost
                    get_mut_node!(self.nmap, &dnid).put();
                    get_mut_node!(self.nmap, &nid).put();
                    return Err(e);
                }
            } else {
                get_mut_node!(self.nmap, &enid).put();
            }
        }

        let offset = match self.find_slot(
            dnid,
            2 + util::div_round_up!(utf::utf16_length(&name), exfatfs::EXFAT_ENAME_MAX),
        ) {
            Ok(v) => v,
            Err(e) => {
                get_mut_node!(self.nmap, &dnid).put();
                get_mut_node!(self.nmap, &nid).put();
                return Err(e);
            }
        };
        if let Err(e) = self.rename_entry(old_dnid, dnid, nid, &name, offset) {
            get_mut_node!(self.nmap, &dnid).put();
            get_mut_node!(self.nmap, &nid).put();
            return Err(e);
        }
        if let Err(e) = self.flush_node(dnid) {
            get_mut_node!(self.nmap, &dnid).put();
            get_mut_node!(self.nmap, &nid).put();
            return Err(e);
        }
        get_mut_node!(self.nmap, &dnid).put();
        get_mut_node!(self.nmap, &nid).put();
        // node itself is not marked as dirty, no need to flush it
        Ok(nid)
    }

    fn unlink_rename_target(&mut self, enid: node::Nid, nid: node::Nid) -> nix::Result<()> {
        let existing = get_node!(self.nmap, &enid);
        assert!(existing.references > 0);
        if existing.is_directory() {
            if get_node!(self.nmap, &nid).is_directory() {
                if let Err(e) = self.rmdir(enid) {
                    if let Some(node) = self.nmap.get_mut(&enid) {
                        node.put();
                    }
                    return Err(e);
                }
                Ok(())
            } else {
                get_mut_node!(self.nmap, &enid).put();
                Err(nix::errno::Errno::ENOTDIR)
            }
        } else if true {
            if !get_node!(self.nmap, &nid).is_directory() {
                if let Err(e) = self.unlink(enid) {
                    if let Some(node) = self.nmap.get_mut(&enid) {
                        node.put();
                    }
                    return Err(e);
                }
                Ok(())
            } else {
                get_mut_node!(self.nmap, &enid).put();
                Err(nix::errno::Errno::EISDIR)
            }
        } else {
            unreachable!();
        }
    }

    #[must_use]
    pub fn get_label(&self) -> &str {
        &self.strlabel
    }

    fn find_label(&mut self) -> nix::Result<u64> {
        let mut offset = 0;
        loop {
            let entry = &self.read_entries(node::NID_ROOT, 1, offset)?[0];
            if entry.typ == exfatfs::EXFAT_ENTRY_LABEL {
                return Ok(offset);
            }
            offset += exfatfs::EXFAT_ENTRY_SIZE_U64;
        }
    }

    pub fn set_label(&mut self, label: &str) -> nix::Result<()> {
        let label = label.as_bytes();
        let label_utf16 = utf::utf8_to_utf16(label, exfatfs::EXFAT_ENAME_MAX, label.len())?;

        let offset = match self.find_label() {
            Ok(v) => v,
            Err(nix::errno::Errno::ENOENT) => self.find_slot(node::NID_ROOT, 1)?,
            Err(e) => return Err(e),
        };

        let mut entry = exfatfs::ExfatEntryLabel::new();
        entry.typ = exfatfs::EXFAT_ENTRY_LABEL;
        entry.length = utf::utf16_length(&label_utf16).try_into().unwrap();
        entry.name.copy_from_slice(&label_utf16);
        if entry.length == 0 {
            entry.typ ^= exfatfs::EXFAT_ENTRY_VALID;
        }

        let entry: &exfatfs::ExfatEntry = bytemuck::cast_ref(&entry);
        self.write_entries(node::NID_ROOT, &[*entry], 1, offset)?;
        self.init_strlabel(label);
        Ok(())
    }

    pub fn opendir_cursor(&mut self, dnid: node::Nid) -> nix::Result<ExfatCursor> {
        get_mut_node!(self.nmap, &dnid).get();
        if let Err(e) = self.cache_directory(dnid) {
            get_mut_node!(self.nmap, &dnid).put();
            return Err(e);
        }
        Ok(ExfatCursor::new(dnid))
    }

    pub fn closedir_cursor(&mut self, c: ExfatCursor) {
        get_mut_node!(self.nmap, &c.pnid).put();
    }

    pub fn readdir_cursor(&mut self, c: &mut ExfatCursor) -> nix::Result<node::Nid> {
        if c.curnid == node::NID_INVALID {
            let dnode = get_node!(self.nmap, &c.pnid);
            if dnode.cnids.is_empty() {
                c.curidx = usize::MAX;
                c.curnid = node::NID_INVALID;
            } else {
                c.curidx = 0;
                c.curnid = dnode.cnids[c.curidx];
            }
        } else {
            let dnode = get_node!(self.nmap, &c.pnid);
            if c.curidx + 1 >= dnode.cnids.len() {
                c.curidx = usize::MAX;
                c.curnid = node::NID_INVALID;
            } else {
                c.curidx += 1;
                c.curnid = dnode.cnids[c.curidx];
            }
        }
        if c.curnid != node::NID_INVALID {
            let node = get_mut_node!(self.nmap, &c.curnid);
            node.get(); // caller needs to put this node
            assert_eq!(node.nid, c.curnid);
            Ok(node.nid)
        } else {
            Err(nix::errno::Errno::ENOENT)
        }
    }

    fn compare_char(&self, a: u16, b: u16) -> bool {
        self.upcase[usize::from(a)] == self.upcase[usize::from(b)]
    }

    fn compare_name(&self, a: &[u16], b: &[u16]) -> bool {
        assert_ne!(a.len(), 0);
        assert_ne!(b.len(), 0);
        let mut i = 0;
        while i < a.len() && i < b.len() {
            if !self.compare_char(u16::from_le(a[i]), u16::from_le(b[i])) {
                return false;
            }
            i += 1;
        }
        utf::utf16_length(a) == utf::utf16_length(b)
    }

    // caller needs to put returned nid
    fn lookup_name(&mut self, dnid: node::Nid, name: &str, n: usize) -> nix::Result<node::Nid> {
        let buf = utf::utf8_to_utf16(name.as_bytes(), EXFAT_NAME_MAX, n)?;
        let mut c = self.opendir_cursor(dnid)?;
        loop {
            let nid = match self.readdir_cursor(&mut c) {
                Ok(v) => v,
                Err(e) => {
                    self.closedir_cursor(c);
                    return Err(e);
                }
            };
            if self.compare_name(&buf, &get_node!(self.nmap, &nid).name) {
                self.closedir_cursor(c);
                return Ok(nid);
            }
            get_mut_node!(self.nmap, &nid).put();
        }
    }

    fn get_comp(path: &str) -> Vec<&str> {
        let mut v = vec![];
        for x in &path.trim_matches('/').split('/').collect::<Vec<&str>>() {
            // multiple /'s between components generates ""
            if !x.is_empty() {
                v.push(*x);
            }
        }
        v
    }

    pub fn lookup(&mut self, path: &str) -> nix::Result<node::Nid> {
        self.lookup_at(node::NID_ROOT, path)
    }

    // Unlike obscure path based abstraction provided by libfuse,
    // lookup ops in fuser simply takes component name with parent ino.
    pub fn lookup_at(&mut self, dnid: node::Nid, path: &str) -> nix::Result<node::Nid> {
        let mut dnid = dnid;
        get_mut_node!(self.nmap, &dnid).get();
        for s in &Self::get_comp(path) {
            if *s == "." {
                continue; // skip "." component
            }
            let nid = match self.lookup_name(dnid, s, s.len()) {
                Ok(v) => v,
                Err(e) => {
                    get_mut_node!(self.nmap, &dnid).put();
                    return Err(e);
                }
            };
            get_mut_node!(self.nmap, &dnid).put();
            dnid = nid; // nid is directory unless last
        }
        Ok(dnid) // dnid isn't necessarily directory
    }

    fn is_allowed_char(comp: &[u8], length: usize) -> bool {
        for x in comp.iter().take(length) {
            if *x >= 0x01 && *x <= 0x1F {
                return false;
            }
            let x = *x as char;
            match x {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => return false,
                _ => (),
            }
        }
        true
    }

    fn split_at(
        &mut self,
        dnid: node::Nid,
        path: &str,
    ) -> nix::Result<(node::Nid, node::Nid, Vec<u16>)> {
        let mut dnid = dnid;
        get_mut_node!(self.nmap, &dnid).get();
        let comp = Self::get_comp(path);
        for (i, s) in comp.iter().enumerate() {
            if *s == "." {
                continue; // skip "." component
            }
            if i == comp.len() - 1 {
                let b = s.as_bytes();
                if !Self::is_allowed_char(b, b.len()) {
                    // contains characters that are not allowed
                    get_mut_node!(self.nmap, &dnid).put();
                    return Err(nix::errno::Errno::ENOENT);
                }
                let name = match utf::utf8_to_utf16(b, EXFAT_NAME_MAX, b.len()) {
                    Ok(v) => v,
                    Err(e) => {
                        get_mut_node!(self.nmap, &dnid).put();
                        return Err(e);
                    }
                };
                let nid = match self.lookup_name(dnid, s, s.len()) {
                    Ok(v) => v,
                    Err(nix::errno::Errno::ENOENT) => node::NID_INVALID,
                    Err(e) => {
                        get_mut_node!(self.nmap, &dnid).put();
                        return Err(e);
                    }
                };
                return Ok((dnid, nid, name)); // caller needs to put both dnid and nid
            }
            let nid = match self.lookup_name(dnid, s, s.len()) {
                Ok(v) => v,
                Err(e) => {
                    get_mut_node!(self.nmap, &dnid).put();
                    return Err(e);
                }
            };
            get_mut_node!(self.nmap, &dnid).put();
            dnid = nid; // nid is directory unless last
        }
        panic!("impossible");
    }

    fn ask_to_fix_(repair: &option::ExfatRepair) -> bool {
        let question = "Fix (Y/N)?";
        match repair {
            option::ExfatRepair::No => false,
            option::ExfatRepair::Yes => {
                println!("{question} Y");
                true
            }
            option::ExfatRepair::Ask => {
                let mut yeah;
                loop {
                    print!("{question} ");
                    std::io::stdout().flush().unwrap();
                    let s = match util::read_line() {
                        Ok(v) => v.to_uppercase(),
                        Err(e) => {
                            eprintln!("{e}");
                            continue;
                        }
                    };
                    let answer = s.trim_end();
                    yeah = answer == "Y";
                    let nope = answer == "N";
                    if yeah || nope {
                        break;
                    }
                }
                yeah
            }
        }
    }

    fn ask_to_fix(&self) -> bool {
        Exfat::ask_to_fix_(&self.opt.repair)
    }

    fn fix_invalid_vbr_checksum(&mut self, vbr_checksum: u32) -> nix::Result<()> {
        let mut sector = vec![0; self.get_sector_size().try_into().unwrap()];
        assert_eq!(sector.len() % std::mem::size_of::<u32>(), 0);
        let x = std::mem::size_of_val(&vbr_checksum);
        let n = sector.len() / x;
        for i in 0..n {
            let offset = x * i;
            byteorder::LittleEndian::write_u32_into(
                &[vbr_checksum.to_le()],
                &mut sector[offset..offset + x],
            );
        }
        if let Err(e) = self.dev.pwrite(&sector, 11 * self.get_sector_size()) {
            log::error!("failed to write correct VBR checksum");
            return Err(util::error2errno(e));
        }
        self.count_errors_fixed();
        Ok(())
    }

    fn fix_invalid_node_checksum(&mut self, nid: node::Nid) -> bool {
        // checksum will be rewritten by exfat_flush_node()
        get_mut_node!(self.nmap, &nid).is_dirty = true;
        self.count_errors_fixed();
        true
    }

    fn fix_unknown_entry(
        &mut self,
        dnid: node::Nid,
        entry: &exfatfs::ExfatEntry,
        offset: u64,
    ) -> nix::Result<()> {
        let mut deleted = *entry;
        deleted.typ &= !exfatfs::EXFAT_ENTRY_VALID;
        let buf: &[u8; exfatfs::EXFAT_ENTRY_SIZE] = bytemuck::cast_ref(&deleted);
        if self.pwrite(dnid, buf, offset)? != exfatfs::EXFAT_ENTRY_SIZE_U64 {
            return Err(nix::errno::Errno::EIO);
        }
        self.count_errors_fixed();
        Ok(())
    }

    fn rootdir_size(&mut self) -> nix::Result<u64> {
        let clusters_max = u32::from_le(self.sb.cluster_count);
        let mut rootdir_cluster = u32::from_le(self.sb.rootdir_cluster);
        let mut clusters = 0;
        // Iterate all clusters of the root directory to calculate its size.
        // It can't be contiguous because there is no flag to indicate this.
        loop {
            if clusters == clusters_max {
                // infinite loop detected
                log::error!("root directory cannot occupy all {clusters} clusters");
                return Err(nix::errno::Errno::EIO);
            }
            if self.cluster_invalid(rootdir_cluster) {
                log::error!("bad cluster {rootdir_cluster:#x} while reading root directory");
                return Err(nix::errno::Errno::EIO);
            }
            rootdir_cluster = self.next_cluster(node::NID_ROOT, rootdir_cluster);
            clusters += 1;
            if rootdir_cluster == exfatfs::EXFAT_CLUSTER_END {
                break;
            }
        }
        if clusters == 0 {
            log::error!("root directory has zero cluster");
            return Err(nix::errno::Errno::EIO);
        }
        Ok(u64::from(clusters) * self.get_cluster_size())
    }

    fn verify_vbr_checksum(&mut self) -> nix::Result<()> {
        let sector_size = self.get_sector_size();
        let sector = match self.dev.preadx(sector_size, 0) {
            Ok(v) => v,
            Err(e) => {
                log::error!("failed to read boot sector");
                return Err(util::error2errno(e));
            }
        };

        let mut vbr_checksum = util::vbr_start_checksum(&sector, sector_size);
        for i in 1..11 {
            let sector = match self.dev.preadx(sector_size, i * sector_size) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("failed to read VBR sector");
                    return Err(util::error2errno(e));
                }
            };
            vbr_checksum = util::vbr_add_checksum(&sector, sector_size, vbr_checksum);
        }

        let sector = match self.dev.preadx(sector_size, 11 * sector_size) {
            Ok(v) => v,
            Err(e) => {
                log::error!("failed to read VBR checksum sector");
                return Err(util::error2errno(e));
            }
        };

        let x = std::mem::size_of_val(&vbr_checksum);
        let n = sector.len() / x;
        for i in 0..n {
            let offset = x * i;
            let c = u32::from_le_bytes(sector[offset..offset + x].try_into().unwrap());
            if c != vbr_checksum {
                log::error!("invalid VBR checksum {c:#x} (expected {vbr_checksum:#x})");
                if !self.ask_to_fix() {
                    return Err(nix::errno::Errno::ECANCELED);
                }
                self.fix_invalid_vbr_checksum(vbr_checksum)?;
            }
        }
        Ok(())
    }

    fn commit_super_block(&mut self) -> nix::Result<()> {
        if let Err(e) = self
            .dev
            .pwrite(unsafe { util::any_as_u8_slice(&self.sb) }, 0)
        {
            log::error!("failed to write super block");
            return Err(util::error2errno(e)); // relan/exfat returns +1
        }
        self.fsync()
    }

    pub fn soil_super_block(&mut self) -> nix::Result<()> {
        if self.ro != 0 {
            return Ok(());
        }
        self.sb.volume_state =
            (u16::from_le(self.sb.volume_state) | exfatfs::EXFAT_STATE_MOUNTED).to_le();
        self.commit_super_block()
    }

    pub fn mount(spec: &str, args: &[&str]) -> nix::Result<Exfat> {
        log::debug!("{args:?}");
        let opt = option::ExfatOption::new(args)?;
        log::debug!("{opt:?}");
        if let Err(e) = time::tzset() {
            log::error!("{e}");
            return Err(nix::errno::Errno::ENXIO);
        }
        time::tzassert();

        let dev = match device::ExfatDevice::new_from_opt(spec, opt.mode) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return Err(nix::errno::Errno::ENODEV); // don't change
            }
        };
        log::debug!("{dev:?}");
        let mut ef = Exfat::new(dev, opt);
        if let option::ExfatMode::Ro = ef.dev.get_mode() {
            ef.ro = match ef.opt.mode {
                option::ExfatMode::Any => -1, // any option -> ro device
                _ => 1,                       // ro option -> ro device
            };
        }
        assert!(ef.ro == 0 || ef.ro == 1 || ef.ro == -1);

        let buf = match ef.dev.preadx(exfatfs::EXFAT_SUPER_BLOCK_SIZE_U64, 0) {
            Ok(v) => v,
            Err(e) => {
                log::error!("failed to read boot sector");
                return Err(util::error2errno(e));
            }
        };
        let (prefix, body, suffix) = unsafe { buf.align_to::<exfatfs::ExfatSuperBlock>() };
        assert!(prefix.is_empty());
        assert!(suffix.is_empty());
        ef.sb = body[0];
        log::debug!("{:?}", ef.sb);

        if ef.sb.oem_name != "EXFAT   ".as_bytes() {
            log::error!("exFAT file system is not found");
            return Err(nix::errno::Errno::EIO);
        }
        // sector cannot be smaller than 512 bytes
        if ef.sb.sector_bits < 9 {
            log::error!("too small sector size: 2^{}", ef.sb.sector_bits);
            return Err(nix::errno::Errno::EIO);
        }
        // officially exFAT supports cluster size up to 32 MB
        if ef.sb.sector_bits + ef.sb.spc_bits > 25 {
            log::error!(
                "too big cluster size: 2^({}+{})",
                ef.sb.sector_bits,
                ef.sb.spc_bits
            );
            return Err(nix::errno::Errno::EIO);
        }

        ef.verify_vbr_checksum()?;

        assert!(ef.zero_cluster.is_empty());
        ef.zero_cluster
            .resize(ef.get_cluster_size().try_into().unwrap(), 0);

        if ef.sb.version_major != 1 || ef.sb.version_minor != 0 {
            log::error!(
                "unsupported exFAT version: {}.{}",
                ef.sb.version_major,
                ef.sb.version_minor
            );
            return Err(nix::errno::Errno::EIO);
        }
        if ef.sb.fat_count != 1 {
            log::error!("unsupported FAT count: {}", ef.sb.fat_count);
            return Err(nix::errno::Errno::EIO);
        }
        if u64::from_le(ef.sb.sector_count) * ef.get_sector_size() > ef.dev.get_size() {
            // this can cause I/O errors later but we don't fail mounting to let
            // user rescue data
            log::warn!(
                "file system in sectors is larger than device: {} * {} > {}",
                u64::from_le(ef.sb.sector_count),
                ef.get_sector_size(),
                ef.dev.get_size()
            );
        }
        if u64::from_le(ef.sb.cluster_count.into()) * ef.get_cluster_size() > ef.dev.get_size() {
            log::error!(
                "file system in clusters is larger than device: {} * {} > {}",
                u64::from_le(ef.sb.cluster_count.into()),
                ef.get_cluster_size(),
                ef.dev.get_size()
            );
            return Err(nix::errno::Errno::EIO);
        }
        if u16::from_le(ef.sb.volume_state) & exfatfs::EXFAT_STATE_MOUNTED != 0 {
            log::warn!("volume was not unmounted cleanly");
        }

        let root = node::ExfatNode::new_root();
        assert_eq!(root.nid, node::NID_ROOT);
        assert_eq!(root.pnid, node::NID_INVALID);
        let nid = root.nid;
        assert!(ef.nmap.insert(nid, root).is_none());

        let root = get_mut_node!(ef.nmap, &nid);
        root.attrib = exfatfs::EXFAT_ATTRIB_DIR;
        root.start_cluster = u32::from_le(ef.sb.rootdir_cluster);
        root.fptr_cluster = root.start_cluster;
        let valid_size = match ef.rootdir_size() {
            Ok(v) => v,
            Err(e) => {
                assert!(ef.nmap.remove(&nid).is_some());
                return Err(e);
            }
        };
        let root = get_mut_node!(ef.nmap, &nid);
        root.valid_size = valid_size;
        root.size = root.valid_size;
        // exFAT does not have time attributes for the root directory
        root.mtime = 0;
        root.atime = 0;
        // always keep at least 1 reference to the root node
        root.get();

        if let Err(e) = ef.cache_directory(nid) {
            get_mut_node!(ef.nmap, &nid).put();
            ef.reset_cache();
            assert!(ef.nmap.remove(&nid).is_some());
            return Err(e);
        }
        if ef.upcase.is_empty() {
            log::error!("upcase table is not found");
            get_mut_node!(ef.nmap, &nid).put();
            ef.reset_cache();
            assert!(ef.nmap.remove(&nid).is_some());
            return Err(nix::errno::Errno::EIO);
        }
        if ef.cmap.chunk.is_empty() {
            log::error!("clusters bitmap is not found");
            get_mut_node!(ef.nmap, &nid).put();
            ef.reset_cache();
            assert!(ef.nmap.remove(&nid).is_some());
            return Err(nix::errno::Errno::EIO);
        }
        Ok(ef)
    }

    fn finalize_super_block(&mut self) -> nix::Result<()> {
        if self.ro != 0 {
            return Ok(());
        }
        self.sb.volume_state =
            (u16::from_le(self.sb.volume_state) & !exfatfs::EXFAT_STATE_MOUNTED).to_le();
        // Some implementations set the percentage of allocated space to 0xff
        // on FS creation and never update it. In this case leave it as is.
        if self.sb.allocated_percent != 0xff {
            let free = self.get_free_clusters();
            let total = u32::from_le(self.sb.cluster_count);
            self.sb.allocated_percent = (((total - free) * 100 + total / 2) / total)
                .try_into()
                .unwrap();
        }
        self.commit_super_block()
    }

    pub fn unmount(&mut self) -> nix::Result<()> {
        self.flush_nodes()?;
        self.flush()?;
        get_mut_node!(self.nmap, &node::NID_ROOT).put();
        self.reset_cache();
        self.dump_nmap();
        assert!(self.nmap.remove(&node::NID_ROOT).is_some());
        assert!(self.nmap.is_empty());
        self.finalize_super_block()?;
        Ok(())
    }

    pub fn stat(&self, nid: node::Nid) -> nix::Result<ExfatStat> {
        let Some(node) = self.nmap.get(&nid) else {
            return Err(nix::errno::Errno::ENOENT);
        };
        let mode = if (node.attrib & exfatfs::EXFAT_ATTRIB_DIR) != 0 {
            libc::S_IFDIR | (0o777 & !self.opt.dmask)
        } else {
            libc::S_IFREG | (0o777 & !self.opt.fmask)
        };
        // There is no such thing as inode in exFAT, but since FUSE ops
        // in fuser are built around ino (which is usually inode#),
        // return nid as ino.
        Ok(ExfatStat {
            st_dev: 0,
            st_ino: node.nid,
            st_nlink: 1,
            st_mode: mode,
            st_uid: self.opt.uid,
            st_gid: self.opt.gid,
            st_rdev: 0,
            st_size: node.size,
            st_blksize: 0,
            st_blocks: util::round_up!(node.size, self.get_cluster_size()) / 512,
            st_atime: node.atime,
            st_mtime: node.mtime,
            // set ctime to mtime to ensure we don't break programs that rely on ctime
            // (e.g. rsync)
            st_ctime: node.mtime,
        })
    }

    // f_files, f_ffree are fake values because in exFAT there is
    // a) no simple way to count files;
    // b) no such thing as inode;
    // So here we assume that inode = cluster.
    #[must_use]
    pub fn statfs(&self) -> ExfatStatFs {
        let cluster_size = self.get_cluster_size().try_into().unwrap();
        let free_clusters = self.get_free_clusters().into();
        ExfatStatFs {
            f_bsize: cluster_size,
            f_blocks: u64::from_le(self.sb.sector_count) >> self.sb.spc_bits,
            f_bfree: free_clusters,
            f_bavail: free_clusters,
            f_files: u32::from_le(self.sb.cluster_count).into(),
            f_ffree: free_clusters,
            f_namelen: EXFAT_NAME_MAX.try_into().unwrap(),
            f_frsize: cluster_size,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_exfat_get_comp() {
        let v = super::Exfat::get_comp("");
        assert_eq!(v.len(), 0);

        let v = super::Exfat::get_comp("file");
        assert_eq!(v, ["file"]);

        let v = super::Exfat::get_comp("/");
        assert_eq!(v.len(), 0);
        let v = super::Exfat::get_comp("//");
        assert_eq!(v.len(), 0);

        let v = super::Exfat::get_comp("/path");
        assert_eq!(v, ["path"]);
        let v = super::Exfat::get_comp("//path");
        assert_eq!(v, ["path"]);

        let v = super::Exfat::get_comp("/path/to/file");
        assert_eq!(v, ["path", "to", "file"]);
        let v = super::Exfat::get_comp("/path/to/file/");
        assert_eq!(v, ["path", "to", "file"]);
        let v = super::Exfat::get_comp("///path///to///file///");
        assert_eq!(v, ["path", "to", "file"]);
    }

    #[allow(unreachable_code)]
    #[test]
    fn test_ask_to_fix() {
        return; // disabled
        loop {
            println!("enter y or Y");
            if super::Exfat::ask_to_fix_(&super::option::ExfatRepair::Ask) {
                break;
            }
        }
        loop {
            println!("enter n or N");
            if !super::Exfat::ask_to_fix_(&super::option::ExfatRepair::Ask) {
                break;
            }
        }
    }
}
