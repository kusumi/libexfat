#[allow(unused_macros)]
#[macro_export]
macro_rules! div_round_up {
    ($x:expr, $d:expr) => {
        $x.div_ceil($d)
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

// div_floor is nightly-only as of 1.84.1
#[allow(unused_macros)]
#[macro_export]
macro_rules! div_round_down {
    ($x:expr, $d:expr) => {
        $x / $d
    };
}
pub use div_round_down;

#[allow(unused_macros)]
#[macro_export]
macro_rules! round_down {
    ($x:expr, $d:expr) => {
        $crate::div_round_down!($x, $d) * $d
    };
}
pub use round_down;

fn add_checksum_byte(sum: u16, byte: u8) -> u16 {
    (u32::from(sum.rotate_right(1)) + u32::from(byte)) as u16
}

fn add_checksum_bytes(sum: u16, buf: &[u8], n: usize) -> u16 {
    let mut sum = sum;
    for b in buf.iter().take(n) {
        sum = add_checksum_byte(sum, *b);
    }
    sum
}

// relan/exfat takes exfat_entry_meta1*
fn start_checksum(entry: &crate::fs::ExfatEntry) -> u16 {
    let buf: &[u8; crate::fs::EXFAT_ENTRY_SIZE] = bytemuck::cast_ref(entry);
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
    add_checksum_bytes(sum, entry, crate::fs::EXFAT_ENTRY_SIZE)
}

pub(crate) fn calc_checksum(entries: &[crate::fs::ExfatEntry], n: usize) -> u16 {
    let mut checksum = start_checksum(&entries[0]);
    for x in entries.iter().take(n).skip(1) {
        let buf: &[u8; crate::fs::EXFAT_ENTRY_SIZE] = bytemuck::cast_ref(x);
        checksum = add_checksum(buf, checksum);
    }
    checksum.to_le()
}

/// # Panics
#[must_use]
pub fn vbr_start_checksum(sector: &[u8], size: u64) -> u32 {
    let mut sum = 0u32;
    for (i, x) in sector.iter().enumerate().take(size.try_into().unwrap()) {
        // skip volume_state and allocated_percent fields
        if i != 0x6a && i != 0x6b && i != 0x70 {
            sum = sum.rotate_right(1) + u32::from(*x);
        }
    }
    sum
}

/// # Panics
#[must_use]
pub fn vbr_add_checksum(sector: &[u8], size: u64, sum: u32) -> u32 {
    let mut sum = sum;
    for x in sector.iter().take(size.try_into().unwrap()) {
        sum = sum.rotate_right(1) + u32::from(*x);
    }
    sum
}

pub(crate) fn calc_name_hash(upcase: &[u16], name: &[u16], length: usize) -> u16 {
    let mut hash = 0u16;
    for x in name.iter().take(length) {
        let c = u16::from_le(*x);
        // convert to upper case
        let c = upcase[usize::from(c)];
        hash = hash.rotate_right(1) + (c & 0xff);
        hash = hash.rotate_right(1) + (c >> 8);
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

pub(crate) fn read_line() -> std::io::Result<String> {
    let mut s = String::new();
    std::io::stdin().read_line(&mut s)?;
    Ok(s)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_div_round_up() {
        assert_eq!(super::div_round_up!(1_u32, 1), 1);
        assert_eq!(super::div_round_up!(2_u32, 1), 2);
        assert_eq!(super::div_round_up!(1024_u32, 1024), 1);
        assert_eq!(super::div_round_up!(1025_u32, 1024), 2);
    }

    #[test]
    fn test_round_up() {
        assert_eq!(super::round_up!(1_u32, 1), 1);
        assert_eq!(super::round_up!(2_u32, 1), 2);
        assert_eq!(super::round_up!(1024_u32, 1024), 1024);
        assert_eq!(super::round_up!(1025_u32, 1024), 2048);
    }

    #[test]
    fn test_div_round_down() {
        assert_eq!(super::div_round_down!(1_u32, 1), 1);
        assert_eq!(super::div_round_down!(2_u32, 1), 2);
        assert_eq!(super::div_round_down!(1024_u32, 1024), 1);
        assert_eq!(super::div_round_down!(1025_u32, 1024), 1);
    }

    #[test]
    fn test_round_down() {
        assert_eq!(super::round_down!(1_u32, 1), 1);
        assert_eq!(super::round_down!(2_u32, 1), 2);
        assert_eq!(super::round_down!(1024_u32, 1024), 1024);
        assert_eq!(super::round_down!(1025_u32, 1024), 1024);
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
}
