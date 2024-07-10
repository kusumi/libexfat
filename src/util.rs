use crate::exfatfs;

use std::io::Seek;

#[allow(unused_macros)]
#[macro_export]
macro_rules! div_round_up {
    ($x:expr, $d:expr) => {
        ($x + $d - 1) / $d
    };
}
pub use div_round_up;

#[allow(unused_macros)]
#[macro_export]
macro_rules! round_up {
    ($x:expr, $d:expr) => {
        $crate::div_round_up!($x, $d) * $d
    };
}
pub use round_up;

fn add_checksum_byte(sum: u16, byte: u8) -> u16 {
    (u32::from((sum << 15) | (sum >> 1)) + u32::from(byte)) as u16
}

fn add_checksum_bytes(sum: u16, buf: &[u8], n: usize) -> u16 {
    let mut sum = sum;
    for b in buf.iter().take(n) {
        sum = add_checksum_byte(sum, *b);
    }
    sum
}

// relan/exfat takes exfat_entry_meta1*
fn start_checksum(entry: &exfatfs::ExfatEntry) -> u16 {
    let buf: &[u8; exfatfs::EXFAT_ENTRY_SIZE] = bytemuck::cast_ref(entry);
    let mut sum = 0;
    for (i, b) in buf.iter().enumerate() {
        // skip checksum field itself
        if i != 2 && i != 3 {
            sum = add_checksum_byte(sum, *b);
        }
    }
    sum
}

fn add_checksum(entry: &[u8], sum: u16) -> u16 {
    add_checksum_bytes(sum, entry, exfatfs::EXFAT_ENTRY_SIZE)
}

pub(crate) fn calc_checksum(entries: &[exfatfs::ExfatEntry], n: usize) -> u16 {
    let mut checksum = start_checksum(&entries[0]);
    for x in entries.iter().take(n).skip(1) {
        let buf: &[u8; exfatfs::EXFAT_ENTRY_SIZE] = bytemuck::cast_ref(x);
        checksum = add_checksum(buf, checksum);
    }
    checksum.to_le()
}

#[must_use]
pub fn vbr_start_checksum(sector: &[u8], size: u64) -> u32 {
    let mut sum = 0;
    for (i, x) in sector.iter().enumerate().take(size.try_into().unwrap()) {
        // skip volume_state and allocated_percent fields
        if i != 0x6a && i != 0x6b && i != 0x70 {
            sum = ((sum << 31) | (sum >> 1)) + u32::from(*x);
        }
    }
    sum
}

#[must_use]
pub fn vbr_add_checksum(sector: &[u8], size: u64, sum: u32) -> u32 {
    let mut sum = sum;
    for x in sector.iter().take(size.try_into().unwrap()) {
        sum = ((sum << 31) | (sum >> 1)) + u32::from(*x);
    }
    sum
}

pub(crate) fn calc_name_hash(upcase: &[u16], name: &[u16], length: usize) -> u16 {
    let mut hash = 0;
    for x in name.iter().take(length) {
        let c = u16::from_le(*x);
        // convert to upper case
        let c = upcase[usize::from(c)];
        hash = ((hash << 15) | (hash >> 1)) + (c & 0xff);
        hash = ((hash << 15) | (hash >> 1)) + (c >> 8);
    }
    hash.to_le()
}

#[must_use]
pub fn humanize_bytes(value: u64) -> (u64, String) {
    // 16 EB (minus 1 byte) is the largest size that can be represented by uint64_t
    let units = ["bytes", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut i = 0;
    let mut divisor = 1;
    let mut temp;
    loop {
        temp = (value + divisor / 2) / divisor;
        if temp == 0 {
            break;
        } else if temp / 1024 * 1024 == temp {
            i += 1;
            divisor *= 1024;
            continue;
        } else if temp < 10240 {
            break;
        }
        i += 1;
        divisor *= 1024;
    }
    (temp, units[i].to_string())
}

pub(crate) fn bin_to_string(b: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    let mut v = vec![];
    for x in b {
        if *x == 0 {
            break;
        }
        v.push(*x);
    }
    String::from_utf8(v)
}

pub(crate) fn get_current_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub(crate) fn seek_set(fp: &mut std::fs::File, offset: u64) -> std::io::Result<u64> {
    fp.seek(std::io::SeekFrom::Start(offset))
}

pub(crate) fn seek_end(fp: &mut std::fs::File, offset: i64) -> std::io::Result<u64> {
    fp.seek(std::io::SeekFrom::End(offset))
}

pub(crate) fn seek_cur(fp: &mut std::fs::File, offset: i64) -> std::io::Result<u64> {
    fp.seek(std::io::SeekFrom::Current(offset))
}

pub(crate) fn read_line() -> std::io::Result<String> {
    let mut s = String::new();
    std::io::stdin().read_line(&mut s)?;
    Ok(s)
}

pub(crate) unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts(
        std::ptr::from_ref::<T>(p).cast::<u8>(),
        ::core::mem::size_of::<T>(),
    )
}

#[must_use]
pub fn error2errno(e: std::io::Error) -> nix::errno::Errno {
    match nix::errno::Errno::try_from(e) {
        Ok(v) => v,
        Err(e) => {
            log::error!("{e}");
            nix::errno::Errno::EIO
        }
    }
}

pub(crate) fn get_os_name() -> &'static str {
    std::env::consts::OS
}

pub(crate) fn is_linux() -> bool {
    get_os_name() == "linux"
}

pub(crate) fn is_freebsd() -> bool {
    get_os_name() == "freebsd"
}

pub(crate) fn is_solaris() -> bool {
    get_os_name() == "solaris"
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_div_round_up() {
        assert_eq!(super::div_round_up!(1, 1), 1);
        assert_eq!(super::div_round_up!(2, 1), 2);
        assert_eq!(super::div_round_up!(1024, 1024), 1);
        assert_eq!(super::div_round_up!(1025, 1024), 2);
    }

    #[test]
    fn test_round_up() {
        assert_eq!(super::round_up!(1, 1), 1);
        assert_eq!(super::round_up!(2, 1), 2);
        assert_eq!(super::round_up!(1024, 1024), 1024);
        assert_eq!(super::round_up!(1025, 1024), 2048);
    }

    #[test]
    fn test_humanize_bytes() {
        let (value, unit) = super::humanize_bytes(0);
        assert_eq!(value, 0);
        assert_eq!(unit, "bytes");

        let (value, unit) = super::humanize_bytes(1023);
        assert_eq!(value, 1023);
        assert_eq!(unit, "bytes");
        let (value, unit) = super::humanize_bytes(1024);
        assert_eq!(value, 1);
        assert_eq!(unit, "KB");
        let (value, unit) = super::humanize_bytes(1025);
        assert_eq!(value, 1025);
        assert_eq!(unit, "bytes");
        let (value, unit) = super::humanize_bytes(2047);
        assert_eq!(value, 2047);
        assert_eq!(unit, "bytes");
        let (value, unit) = super::humanize_bytes(2048);
        assert_eq!(value, 2);
        assert_eq!(unit, "KB");
        let (value, unit) = super::humanize_bytes(2049);
        assert_eq!(value, 2049);
        assert_eq!(unit, "bytes");

        let (value, unit) = super::humanize_bytes(1 << 20);
        assert_eq!(value, 1);
        assert_eq!(unit, "MB");
        let (value, unit) = super::humanize_bytes(1 << 30);
        assert_eq!(value, 1);
        assert_eq!(unit, "GB");
        let (value, unit) = super::humanize_bytes(1 << 40);
        assert_eq!(value, 1);
        assert_eq!(unit, "TB");
        let (value, unit) = super::humanize_bytes(1 << 50);
        assert_eq!(value, 1);
        assert_eq!(unit, "PB");
        let (value, unit) = super::humanize_bytes(1 << 60);
        assert_eq!(value, 1);
        assert_eq!(unit, "EB");
    }

    #[test]
    fn test_bin_to_string() {
        assert_eq!(
            super::bin_to_string(&[101, 120, 70, 65, 84]),
            Ok("exFAT".to_string())
        );
        assert_eq!(
            super::bin_to_string(&[101, 120, 70, 65, 84, 0]),
            Ok("exFAT".to_string())
        );
        assert_eq!(
            super::bin_to_string(&[101, 120, 70, 65, 84, 0, 0]),
            Ok("exFAT".to_string())
        );

        assert_eq!(super::bin_to_string(&[0]), Ok(String::new()));
        assert_eq!(super::bin_to_string(&[0, 0]), Ok(String::new()));
        assert_eq!(
            super::bin_to_string(&[0, 0, 101, 120, 70, 65, 84]),
            Ok(String::new())
        );
    }

    #[test]
    fn test_get_current_time() {
        let t1 = super::get_current_time();
        std::thread::sleep(std::time::Duration::from_secs(1));
        let t2 = super::get_current_time();
        assert_ne!(t1, t2);
        assert!(t2 > t1);
    }
}