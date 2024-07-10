use crate::bitmap;
use crate::exfat;
use crate::exfatfs;
use crate::node;
use crate::util;

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

    pub(crate) fn init_strlabel(&mut self, b: &[u8]) {
        self.strlabel = util::bin_to_string(b).unwrap();
    }

    pub(crate) fn alloc_node(&mut self) -> node::ExfatNode {
        assert!(self.nid_next > node::NID_ROOT);
        assert_ne!(self.nid_next, node::Nid::MAX, "Nid exhausted");
        let nid = self.nid_next;
        self.nid_next += 1;
        node::ExfatNode::new(nid)
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
    pub fn bmap_exists(&self, index: usize) -> bool {
        bitmap::bmap_get(&self.cmap.chunk, index) != 0
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

    pub(crate) fn dump_nmap(&self) {
        self.dump_nmap_impl(node::NID_ROOT, 0);
    }
}
