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

use std::fmt::Display;

pub const VERSION: [i32; 3] = [
    1, 4, 0, // from relan/exfat: libexfat/config.h:#define VERSION "1.4.0"
];

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Error(std::io::Error),
    Errno(nix::errno::Errno),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Error(e) => write!(f, "{e}"),
            Self::Errno(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Error(e)
    }
}

impl From<nix::errno::Errno> for Error {
    fn from(e: nix::errno::Errno) -> Self {
        Self::Errno(e)
    }
}

/// # Errors
pub fn mount(spec: &str, args: &[&str]) -> Result<exfat::Exfat> {
    exfat::Exfat::mount(spec, args)
}

/// # Errors
pub fn open(spec: &str, mode: &str) -> Result<device::Device> {
    device::Device::new(spec, mode)
}
