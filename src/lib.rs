pub mod bitmap;
pub mod device;
pub mod exfat;
pub mod exfatfs;
mod extra;
pub mod node;
mod option;
mod time;
pub mod utf;
pub mod util;

pub const VERSION: [i32; 3] = [
    1, 4, 0, // from relan/exfat: libexfat/config.h:#define VERSION "1.4.0"
];

pub fn mount(spec: &str, args: &[&str]) -> nix::Result<exfat::Exfat> {
    exfat::Exfat::mount(spec, args)
}

pub fn open(spec: &str, mode: &str) -> std::io::Result<device::ExfatDevice> {
    device::ExfatDevice::new(spec, mode)
}
