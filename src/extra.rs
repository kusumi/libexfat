use crate::bitmap;
use crate::exfat;
use crate::exfatfs;
use crate::node;
use crate::option;
use crate::util;

#[cfg(feature = "bitmap_u64")]
use byteorder::ByteOrder;

impl exfat::ExfatClusterMap {
    #[cfg(not(feature = "bitmap_u64"))]
    pub(crate) fn get(&mut self) -> &[u8] {
        &self.chunk
    }

    #[cfg(feature = "bitmap_u64")]
    pub(crate) fn get(&mut self) -> &[u8] {
        if self.bytes.is_empty() {
            self.bytes = vec![0; bitmap::size(self.count.try_into().unwrap())];
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
            self.chunk = bitmap::alloc(self.count.try_into().unwrap());
        }
        byteorder::LittleEndian::read_u64_into(&chunk, &mut self.chunk);
    }
}

impl exfat::Exfat {
    #[must_use]
    pub fn get_super_block(&self) -> exfatfs::ExfatSuperBlock {
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
        self.strlabel = util::bin_to_string(b).unwrap();
    }

    #[must_use]
    pub fn get_label(&self) -> &str {
        &self.strlabel
    }

    pub(crate) fn insert_root_node(&mut self, node: node::ExfatNode) {
        let nid = node.nid;
        assert_eq!(nid, node::NID_ROOT);
        assert!(self.nmap.is_empty());
        assert!(self.nmap.insert(nid, node).is_none());
        if let option::ExfatNidAlloc::Bitmap = self.opt.nidalloc {
            self.set_root_nidmap();
        }
    }

    fn set_root_nidmap(&mut self) {
        assert_eq!(bitmap::count(&self.imap.chunk), 0);
        bitmap::set(&mut self.imap.chunk, node::NID_ROOT.try_into().unwrap());
        assert_eq!(bitmap::count(&self.imap.chunk), 1);
    }

    pub(crate) fn remove_root_node(&mut self) {
        assert!(self.nmap.remove(&node::NID_ROOT).is_some());
        assert!(self.nmap.is_empty());
        if let option::ExfatNidAlloc::Bitmap = self.opt.nidalloc {
            self.clear_root_nidmap();
        }
    }

    fn clear_root_nidmap(&mut self) {
        assert_eq!(bitmap::count(&self.imap.chunk), 1);
        bitmap::clear(&mut self.imap.chunk, node::NID_ROOT.try_into().unwrap());
        assert_eq!(bitmap::count(&self.imap.chunk), 0);
    }

    pub(crate) fn alloc_node(&self) -> node::ExfatNode {
        node::ExfatNode::new(node::NID_INVALID)
    }

    pub(crate) fn get_nid(&mut self) -> nix::Result<node::Nid> {
        assert!(self.imap.next >= node::NID_NODE_OFFSET);
        assert_ne!(self.imap.max, 0);
        let nid = match self.opt.nidalloc {
            option::ExfatNidAlloc::Linear => self.get_nidmap_linear()?,
            option::ExfatNidAlloc::Bitmap => self.get_nidmap_bitmap()?,
        };
        assert_ne!(nid, node::NID_INVALID);
        assert_ne!(nid, node::NID_ROOT);
        Ok(nid)
    }

    fn get_nidmap_linear(&mut self) -> nix::Result<node::Nid> {
        if self.imap.next > self.imap.max {
            return Err(nix::errno::Errno::ENOSPC);
        }
        let nid = self.imap.next;
        self.imap.next += 1;
        Ok(nid)
    }

    fn get_nidmap_bitmap(&mut self) -> nix::Result<node::Nid> {
        if let Some(v) = self.imap.pool.pop() {
            bitmap::set(&mut self.imap.chunk, v.try_into().unwrap());
            return Ok(v); // reuse nid in pool
        }
        if self.imap.next > self.imap.max {
            self.imap.next = node::NID_NODE_OFFSET;
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

    pub(crate) fn put_nid(&mut self, nid: node::Nid) {
        match self.opt.nidalloc {
            option::ExfatNidAlloc::Linear => self.put_nidmap_linear(nid),
            option::ExfatNidAlloc::Bitmap => self.put_nidmap_bitmap(nid),
        }
    }

    fn put_nidmap_linear(&mut self, _nid: node::Nid) {}

    fn put_nidmap_bitmap(&mut self, nid: node::Nid) {
        const NIDMAP_POOL_MAX: usize = 1 << 8;
        bitmap::clear(&mut self.imap.chunk, nid.try_into().unwrap());
        if self.imap.pool.len() < NIDMAP_POOL_MAX {
            self.imap.pool.push(nid);
        }
    }

    #[must_use]
    pub fn get_node(&self, nid: node::Nid) -> Option<&node::ExfatNode> {
        self.nmap.get(&nid)
    }

    pub fn get_mut_node(&mut self, nid: node::Nid) -> Option<&mut node::ExfatNode> {
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

    pub fn fsync(&mut self) -> nix::Result<()> {
        if let Err(e) = self.dev.fsync() {
            log::error!("fsync failed: {e}");
            return Err(util::error2errno(e));
        }
        Ok(())
    }

    #[must_use]
    pub fn is_cluster_allocated(&self, index: usize) -> bool {
        bitmap::get(&self.cmap.chunk, index) != 0
    }

    pub(crate) fn ffas_cluster(&mut self, start: u32, end: u32) -> nix::Result<u32> {
        let index = bitmap::find_and_set(
            &mut self.cmap.chunk,
            start.try_into().unwrap(),
            end.try_into().unwrap(),
        );
        if index == usize::MAX {
            Err(nix::errno::Errno::ENOSPC)
        } else {
            Ok(exfatfs::EXFAT_FIRST_DATA_CLUSTER + u32::try_from(index).unwrap())
        }
    }

    pub(crate) fn ffas_nid(&mut self, start: node::Nid, end: node::Nid) -> nix::Result<node::Nid> {
        let index = bitmap::find_and_set(
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

    pub(crate) fn dump_nmap(&self) {
        self.dump_nmap_impl(node::NID_ROOT, 0);
    }

    fn dump_nmap_impl(&self, nid: node::Nid, depth: usize) {
        let node = exfat::get_node!(self.nmap, &nid);
        log::debug!(
            "{}nid {} pnid {} name \"{}\" ref {}",
            "  ".repeat(depth),
            node.nid,
            node.pnid,
            node.get_name(),
            node.references,
        );
        for x in &node.cnids {
            self.dump_nmap_impl(*x, depth + 1);
        }
    }
}
