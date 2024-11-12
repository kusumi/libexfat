pub mod bitmap;
pub mod ctl;
pub mod device;
pub mod exfat;
mod extra;
pub mod fs;
pub mod node;
mod option;
mod time;
pub mod utf;
pub mod util;

pub const VERSION: [i32; 3] = [
    1, 4, 0, // from relan/exfat: libexfat/config.h:#define VERSION "1.4.0"
];

/// # Errors
pub fn mount(spec: &str, args: &[&str]) -> nix::Result<exfat::Exfat> {
    exfat::Exfat::mount(spec, args)
}

/// # Errors
pub fn open(spec: &str, mode: &str) -> std::io::Result<device::ExfatDevice> {
    device::ExfatDevice::new(spec, mode)
}
