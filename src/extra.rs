#[cfg(feature = "bitmap_u64")]
use byteorder::ByteOrder;

impl crate::fs::ExfatSuperBlock {
    #[must_use]
    pub fn get_sector_size(&self) -> u64 {
        1 << self.sector_bits
    }

    #[must_use]
    pub fn get_cluster_size(&self) -> u64 {
        self.get_sector_size() << self.spc_bits
    }
}

impl crate::exfat::ClusterMap {
    #[cfg(not(feature = "bitmap_u64"))]
    pub(crate) fn get(&mut self) -> &[u8] {
        &self.chunk
    }

    #[cfg(feature = "bitmap_u64")]
    pub(crate) fn get(&mut self) -> &[u8] {
        if self.bytes.is_empty() {
            self.bytes = vec![0; crate::bitmap::size(self.count.try_into().unwrap())];
        }
        byteorder::LittleEndian::write_u64_into(&self.chunk, &mut self.bytes);
        &self.bytes
    }

    #[cfg(not(feature = "bitmap_u64"))]
    pub(crate) fn set(&mut self, chunk: Vec<u8>) {
        self.chunk = chunk;
    }

    #[cfg(feature = "bitmap_u64")]
    pub(crate) fn set(&mut self, chunk: Vec<u8>) {
        if self.chunk.is_empty() {
            self.chunk = crate::bitmap::alloc(self.count.try_into().unwrap());
        }
        byteorder::LittleEndian::read_u64_into(&chunk, &mut self.chunk);
    }
}

impl crate::exfat::Exfat {
    #[must_use]
    pub fn get_super_block(&self) -> crate::fs::ExfatSuperBlock {
        self.sb
    }

    #[must_use]
    pub fn get_sector_size(&self) -> u64 {
        self.sb.get_sector_size()
    }

    #[must_use]
    pub fn get_cluster_size(&self) -> u64 {
        self.sb.get_cluster_size()
    }

    #[must_use]
    pub fn is_readonly(&self) -> bool {
        self.ro != 0 // either 1 or -1
    }

    pub(crate) fn init_label(&mut self, b: &[u8]) {
        self.strlabel = crate::util::bin_to_string(b).unwrap();
    }

    #[must_use]
    pub fn get_label(&self) -> &str {
        &self.strlabel
    }

    pub(crate) fn insert_root_node(&mut self, node: crate::node::Node) {
        let nid = node.nid;
        assert_eq!(nid, crate::node::NID_ROOT);
        assert!(self.nmap.is_empty());
        assert!(self.nmap.insert(nid, node).is_none());
        if let crate::option::NidAllocMode::Bitmap = self.opt.nidalloc {
            self.set_root_nidmap();
        }
    }

    fn set_root_nidmap(&mut self) {
        assert_eq!(crate::bitmap::count(&self.imap.chunk), 0);
        crate::bitmap::set(
            &mut self.imap.chunk,
            crate::node::NID_ROOT.try_into().unwrap(),
        );
        assert_eq!(crate::bitmap::count(&self.imap.chunk), 1);
    }

    pub(crate) fn remove_root_node(&mut self) {
        assert!(self.nmap.remove(&crate::node::NID_ROOT).is_some());
        assert!(self.nmap.is_empty());
        if let crate::option::NidAllocMode::Bitmap = self.opt.nidalloc {
            self.clear_root_nidmap();
        }
    }

    fn clear_root_nidmap(&mut self) {
        assert_eq!(crate::bitmap::count(&self.imap.chunk), 1);
        crate::bitmap::clear(
            &mut self.imap.chunk,
            crate::node::NID_ROOT.try_into().unwrap(),
        );
        assert_eq!(crate::bitmap::count(&self.imap.chunk), 0);
    }

    pub(crate) fn alloc_node() -> crate::node::Node {
        crate::node::Node::new(crate::node::NID_NONE)
    }

    pub(crate) fn alloc_nid(&mut self) -> nix::Result<crate::node::Nid> {
        assert!(self.imap.next >= crate::node::NID_NODE_OFFSET);
        assert_ne!(self.imap.max, 0);
        let nid = match self.opt.nidalloc {
            crate::option::NidAllocMode::Linear => self.alloc_nidmap_linear()?,
            crate::option::NidAllocMode::Bitmap => self.alloc_nidmap_bitmap()?,
        };
        assert_ne!(nid, crate::node::NID_NONE);
        assert_ne!(nid, crate::node::NID_ROOT);
        Ok(nid)
    }

    fn alloc_nidmap_linear(&mut self) -> nix::Result<crate::node::Nid> {
        if self.imap.next > self.imap.max {
            return Err(nix::errno::Errno::ENOSPC);
        }
        let nid = self.imap.next;
        self.imap.next += 1;
        Ok(nid)
    }

    fn alloc_nidmap_bitmap(&mut self) -> nix::Result<crate::node::Nid> {
        if let Some(v) = self.imap.pool.pop() {
            crate::bitmap::set(&mut self.imap.chunk, v.try_into().unwrap());
            return Ok(v); // reuse nid in pool
        }
        if self.imap.next > self.imap.max {
            self.imap.next = crate::node::NID_NODE_OFFSET;
        }
        let hint = self.imap.next;
        self.imap.next += 1;
        let nid = match self.ffas_nid(hint, self.imap.max + 1) {
            Ok(v) => v,
            Err(nix::errno::Errno::ENOSPC) => match self.ffas_nid(0, hint) {
                Ok(v) => v,
                Err(nix::errno::Errno::ENOSPC) => {
                    log::error!("no free space left for node");
                    return Err(nix::errno::Errno::ENOSPC);
                }
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        Ok(nid)
    }

    pub(crate) fn free_nid(&mut self, nid: crate::node::Nid) {
        match self.opt.nidalloc {
            crate::option::NidAllocMode::Linear => (),
            crate::option::NidAllocMode::Bitmap => self.free_nidmap_bitmap(nid),
        }
    }

    fn free_nidmap_bitmap(&mut self, nid: crate::node::Nid) {
        const NIDMAP_POOL_MAX: usize = 1 << 8;
        crate::bitmap::clear(&mut self.imap.chunk, nid.try_into().unwrap());
        if self.imap.pool.len() < NIDMAP_POOL_MAX {
            self.imap.pool.push(nid);
        }
    }

    #[must_use]
    pub fn get_node(&self, nid: crate::node::Nid) -> Option<&crate::node::Node> {
        self.nmap.get(&nid)
    }

    pub fn get_node_mut(&mut self, nid: crate::node::Nid) -> Option<&mut crate::node::Node> {
        self.nmap.get_mut(&nid)
    }

    #[must_use]
    pub fn get_errors(&self) -> usize {
        self.errors // XXX unsupported, always 0
    }

    #[must_use]
    pub fn get_errors_fixed(&self) -> usize {
        self.errors_fixed
    }

    pub(crate) fn count_errors_fixed(&mut self) {
        self.errors_fixed += 1;
    }

    /// # Errors
    pub fn fsync(&mut self) -> crate::Result<()> {
        if let Err(e) = self.dev.fsync() {
            log::error!("fsync failed: {e}");
            return Err(e.into());
        }
        Ok(())
    }

    #[must_use]
    pub fn is_cluster_allocated(&self, index: usize) -> bool {
        crate::bitmap::get(&self.cmap.chunk, index) != 0
    }

    pub(crate) fn ffas_cluster(&mut self, start: u32, end: u32) -> nix::Result<u32> {
        let index = crate::bitmap::find_and_set(
            &mut self.cmap.chunk,
            start.try_into().unwrap(),
            end.try_into().unwrap(),
        );
        if index == usize::MAX {
            Err(nix::errno::Errno::ENOSPC)
        } else {
            Ok(crate::fs::EXFAT_FIRST_DATA_CLUSTER + u32::try_from(index).unwrap())
        }
    }

    pub(crate) fn ffas_nid(
        &mut self,
        start: crate::node::Nid,
        end: crate::node::Nid,
    ) -> nix::Result<crate::node::Nid> {
        let index = crate::bitmap::find_and_set(
            &mut self.imap.chunk,
            start.try_into().unwrap(),
            end.try_into().unwrap(),
        );
        if index == usize::MAX {
            Err(nix::errno::Errno::ENOSPC)
        } else {
            Ok(index.try_into().unwrap())
        }
    }

    /// # Errors
    /// # Panics
    pub fn preadx(
        &mut self,
        nid: crate::node::Nid,
        size: u64,
        offset: u64,
    ) -> crate::Result<Vec<u8>> {
        let mut buf = vec![0; size.try_into().unwrap()];
        let n = self.pread(nid, &mut buf, offset)?;
        Ok(buf[..n.try_into().unwrap()].to_vec())
    }

    /// # Errors
    pub fn read_all(&mut self, nid: crate::node::Nid) -> crate::Result<Vec<u8>> {
        self.preadx(nid, self.stat(nid)?.st_size, 0)
    }

    /// # Errors
    pub fn readdir(&mut self, dnid: crate::node::Nid) -> crate::Result<Vec<crate::node::Nid>> {
        let mut c = self.opendir_cursor(dnid)?;
        let mut v = vec![];
        loop {
            let nid = match self.readdir_cursor(&mut c) {
                Ok(v) => v,
                Err(e) => {
                    if let crate::Error::Errno(e) = e {
                        if e == nix::errno::Errno::ENOENT {
                            break;
                        }
                    }
                    self.closedir_cursor(c);
                    return Err(e);
                }
            };
            v.push(nid);
            crate::exfat::get_node_mut!(self, &nid).put();
        }
        self.closedir_cursor(c);
        Ok(v)
    }

    /// # Errors
    /// # Panics
    pub fn prune_node(&mut self, xnid: crate::node::Nid) -> crate::Result<(usize, usize)> {
        self.flush_nodes()?;
        self.flush()?;

        let a = self.nmap.len();
        self.prune_node_impl(crate::node::NID_ROOT, xnid)?;
        assert!(self.nmap.contains_key(&crate::node::NID_ROOT));
        assert!(self.nmap.contains_key(&xnid));
        let b = self.nmap.len();
        assert!(a >= b);
        let total_pruned = a - b;

        self.dump_node_all();

        let xname = crate::exfat::get_node!(self, &xnid).get_name().to_string();
        self.recache_directory(crate::node::NID_ROOT, &xname)?;
        let c = self.nmap.len();
        assert!(c >= b);
        let total_recached = c - b;

        log::info!("{total_pruned} node pruned, {total_recached} node recached");
        Ok((total_pruned, total_recached))
    }

    // based on reset_node_impl
    fn prune_node_impl(
        &mut self,
        nid: crate::node::Nid,
        xnid: crate::node::Nid,
    ) -> nix::Result<()> {
        for &cnid in &crate::exfat::get_node!(self, &nid).cnids.clone() {
            if cnid == xnid {
                if nid != crate::node::NID_ROOT {
                    return Err(nix::errno::Errno::EBUSY);
                }
                continue;
            }
            match self.prune_node_impl(cnid, xnid) {
                Ok(()) => {
                    self.nmap_detach(nid, cnid)?;
                }
                Err(nix::errno::Errno::EBUSY) => {
                    if nid != crate::node::NID_ROOT {
                        return Err(nix::errno::Errno::EBUSY);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if nid != crate::node::NID_ROOT {
            let node = crate::exfat::get_node_mut!(self, &nid);
            node.is_cached = false;
            assert!(!node.is_dirty, "node '{}' is dirty", node.get_name());
            while node.references > 0 {
                node.put();
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn dump_node(&self, nid: crate::node::Nid) {
        self.dump_node_impl(nid, 0);
    }

    pub(crate) fn dump_node_all(&self) {
        self.dump_node_impl(crate::node::NID_ROOT, 0);
    }

    fn dump_node_impl(&self, nid: crate::node::Nid, depth: usize) {
        let node = crate::exfat::get_node!(self, &nid);
        log::debug!(
            "{}nid {} pnid {} name \"{}\" ref {}",
            "  ".repeat(depth),
            node.nid,
            node.pnid,
            node.get_name(),
            node.references,
        );
        for x in &node.cnids {
            self.dump_node_impl(*x, depth + 1);
        }
    }
}
