const CTL: u8 = b'E';

const CTL_NIDPRUNE: u8 = 0;

pub type CtlNidPruneData = [u64; 2];

pub const CTL_NIDPRUNE_ENCODE: u64 =
    nix::request_code_read!(CTL, CTL_NIDPRUNE, std::mem::size_of::<CtlNidPruneData>());

nix::ioctl_read!(nidprune, CTL, CTL_NIDPRUNE, CtlNidPruneData);
