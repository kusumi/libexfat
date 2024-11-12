pub const EXFAT_CTL: u8 = b'E';

pub const EXFAT_CTL_NIDPRUNE: u8 = 0;

pub type ExfatCtlNidPruneData = [u64; 2];

pub const EXFAT_CTL_NIDPRUNE_ENCODE: u64 = nix::request_code_read!(
    EXFAT_CTL,
    EXFAT_CTL_NIDPRUNE,
    std::mem::size_of::<ExfatCtlNidPruneData>()
);
