use crate::util;

// size_t in relan/exfat, but u8 is easier to handle in Rust.
pub(crate) type Bitmap = u8;

const BITMAP_SIZE_BYTES: usize = std::mem::size_of::<Bitmap>();
const BITMAP_SIZE_BITS: usize = BITMAP_SIZE_BYTES * 8;

#[must_use]
pub fn bmap_alloc(count: usize) -> Vec<Bitmap> {
    vec![0; bmap_size(count) / BITMAP_SIZE_BYTES]
}

#[must_use]
pub(crate) fn bmap_size(count: usize) -> usize {
    util::round_up!(count, BITMAP_SIZE_BITS) / 8 // bytes
}

pub(crate) fn bmap_block(index: usize) -> usize {
    index / BITMAP_SIZE_BITS // Bitmap array index
}

pub(crate) fn bmap_mask(index: usize) -> Bitmap {
    1 << (index % (BITMAP_SIZE_BITS)) // bit within a Bitmap
}

pub(crate) fn bmap_get(bitmap: &[Bitmap], index: usize) -> Bitmap {
    bitmap[bmap_block(index)] & bmap_mask(index)
}

pub fn bmap_set(bitmap: &mut [Bitmap], index: usize) {
    bitmap[bmap_block(index)] |= bmap_mask(index);
}

pub(crate) fn bmap_clr(bitmap: &mut [Bitmap], index: usize) {
    bitmap[bmap_block(index)] &= !bmap_mask(index);
}

pub(crate) fn bmap_find_and_set(bitmap: &mut [Bitmap], start: u32, end: u32) -> u32 {
    let start = start.try_into().unwrap();
    let end = end.try_into().unwrap();
    let start_index = start / BITMAP_SIZE_BITS;
    let end_index = util::div_round_up!(end, BITMAP_SIZE_BITS);

    for i in start_index..end_index {
        if bitmap[i] == Bitmap::MAX {
            continue;
        }
        let start_bitindex = std::cmp::max(i * BITMAP_SIZE_BITS, start);
        let end_bitindex = std::cmp::min((i + 1) * BITMAP_SIZE_BITS, end);
        for c in start_bitindex..end_bitindex {
            if bmap_get(bitmap, c) == 0 {
                bmap_set(bitmap, c);
                return c.try_into().unwrap();
            }
        }
    }
    u32::MAX
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_bmap_size() {
        assert_eq!(super::bmap_size(0), 0);

        assert_eq!(super::bmap_size(1), super::BITMAP_SIZE_BYTES);
        assert_eq!(
            super::bmap_size(super::BITMAP_SIZE_BITS),
            super::BITMAP_SIZE_BYTES
        );

        assert_eq!(
            super::bmap_size(super::BITMAP_SIZE_BITS + 1),
            super::BITMAP_SIZE_BYTES * 2
        );
        assert_eq!(
            super::bmap_size(super::BITMAP_SIZE_BITS * 2),
            super::BITMAP_SIZE_BYTES * 2
        );

        assert_eq!(
            super::bmap_size(super::BITMAP_SIZE_BITS * 2 + 1),
            super::BITMAP_SIZE_BYTES * 3
        );
        assert_eq!(
            super::bmap_size(super::BITMAP_SIZE_BITS * 3),
            super::BITMAP_SIZE_BYTES * 3
        );
    }

    #[test]
    fn test_bmap_block() {
        assert_eq!(super::bmap_block(0), 0);
        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS - 1), 0);

        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS * 2 - 1), 1);

        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS * 2 - 1), 1);

        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS * 2), 2);
        assert_eq!(super::bmap_block(super::BITMAP_SIZE_BITS * 3 - 1), 2);
    }

    #[test]
    fn test_bmap_mask() {
        assert_eq!(super::bmap_mask(0), 1);
        assert_eq!(super::bmap_mask(1), 2);
        assert_eq!(super::bmap_mask(2), 4);
        assert_eq!(super::bmap_mask(3), 8);
        assert_eq!(super::bmap_mask(4), 16);
        assert_eq!(super::bmap_mask(5), 32);
        assert_eq!(super::bmap_mask(6), 64);
        assert_eq!(super::bmap_mask(7), 128);

        if super::BITMAP_SIZE_BYTES != 1 {
            assert_eq!(u64::from(super::bmap_mask(8)), 1 << 8);
            assert_eq!(u64::from(super::bmap_mask(9)), 1 << 9);
            assert_eq!(u64::from(super::bmap_mask(10)), 1 << 10);
            assert_eq!(u64::from(super::bmap_mask(11)), 1 << 11);
            assert_eq!(u64::from(super::bmap_mask(12)), 1 << 12);
            assert_eq!(u64::from(super::bmap_mask(13)), 1 << 13);
            assert_eq!(u64::from(super::bmap_mask(14)), 1 << 14);
            assert_eq!(u64::from(super::bmap_mask(15)), 1 << 15);
        }

        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 1), 2);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 2), 4);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 3), 8);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 4), 16);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 5), 32);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 6), 64);
        assert_eq!(super::bmap_mask(super::BITMAP_SIZE_BITS + 7), 128);

        if super::BITMAP_SIZE_BYTES != 1 {
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 8)),
                1 << 8
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 9)),
                1 << 9
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 10)),
                1 << 10
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 11)),
                1 << 11
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 12)),
                1 << 12
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 13)),
                1 << 13
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 14)),
                1 << 14
            );
            assert_eq!(
                u64::from(super::bmap_mask(super::BITMAP_SIZE_BITS + 15)),
                1 << 15
            );
        }
    }

    #[test]
    fn test_bmap_get() {
        let b = [0];
        assert_eq!(super::bmap_get(&b, 0), 0);
        assert_eq!(super::bmap_get(&b, 1), 0);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let b = [1];
        assert_eq!(super::bmap_get(&b, 0), 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let b = [2];
        assert_eq!(super::bmap_get(&b, 0), 0);
        assert_eq!(super::bmap_get(&b, 1), 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let b = [3];
        assert_eq!(super::bmap_get(&b, 0), 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let b = [4];
        assert_eq!(super::bmap_get(&b, 0), 0);
        assert_eq!(super::bmap_get(&b, 1), 0);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let b = [5];
        assert_eq!(super::bmap_get(&b, 0), 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let b = [6];
        assert_eq!(super::bmap_get(&b, 0), 0);
        assert_eq!(super::bmap_get(&b, 1), 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let b = [7];
        assert_eq!(super::bmap_get(&b, 0), 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        assert_eq!(super::bmap_get(&b, 2), 4);

        let b = [0, 0];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let b = [0, 1];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let b = [0, 2];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let b = [0, 3];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let b = [0, 4];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let b = [0, 5];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let b = [0, 6];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let b = [0, 7];
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
    }

    #[test]
    fn test_bmap_set() {
        let mut b = [0];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [1];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [2];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [3];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [4];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [5];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [6];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);
        let mut b = [7];
        super::bmap_set(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 1);
        super::bmap_set(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 2);
        super::bmap_set(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 4);

        let mut b = [0, 0];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 1];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 2];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 3];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 4];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 5];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 6];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
        let mut b = [0, 7];
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 1);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 2);
        super::bmap_set(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 4);
    }

    #[test]
    fn test_bmap_clr() {
        let mut b = [0];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [1];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [2];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [3];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [4];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [5];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [6];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);
        let mut b = [7];
        super::bmap_clr(&mut b, 0);
        assert_eq!(super::bmap_get(&b, 0), 0);
        super::bmap_clr(&mut b, 1);
        assert_eq!(super::bmap_get(&b, 1), 0);
        super::bmap_clr(&mut b, 2);
        assert_eq!(super::bmap_get(&b, 2), 0);

        let mut b = [0, 0];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 1];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 2];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 3];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 4];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 5];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 6];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
        let mut b = [0, 7];
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 1);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 1), 0);
        super::bmap_clr(&mut b, super::BITMAP_SIZE_BITS + 2);
        assert_eq!(super::bmap_get(&b, super::BITMAP_SIZE_BITS + 2), 0);
    }

    #[test]
    fn test_bmap_find_and_set() {
        let mut b = [0];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 2);
        let mut b = [1];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        let mut b = [2];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        let mut b = [3];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 4);
        let mut b = [4];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        let mut b = [5];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 4);
        let mut b = [6];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 4);
        let mut b = [7];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 4);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 5);
        let mut b = [super::Bitmap::MAX];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        let mut b = [super::Bitmap::MAX - 1];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        let mut b = [super::Bitmap::MAX - 2];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);
        let mut b = [super::Bitmap::MAX - 3];
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::bmap_find_and_set(&mut b, 0, 8), u32::MAX);

        const BITMAP_SIZE_BITS_U32: u32 = super::BITMAP_SIZE_BITS as u32;
        let mut b = [super::Bitmap::MAX, 0];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 2
        );
        let mut b = [super::Bitmap::MAX, 1];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 2
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        let mut b = [super::Bitmap::MAX, 2];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 2
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        let mut b = [super::Bitmap::MAX, 3];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 2
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 4
        );
        let mut b = [super::Bitmap::MAX, 4];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        let mut b = [super::Bitmap::MAX, 5];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 4
        );
        let mut b = [super::Bitmap::MAX, 6];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 4
        );
        let mut b = [super::Bitmap::MAX, 7];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 3
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 4
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 5
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 1];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 2];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 3];
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            BITMAP_SIZE_BITS_U32 + 1
        );
        assert_eq!(
            super::bmap_find_and_set(&mut b, 0, BITMAP_SIZE_BITS_U32 * 2),
            u32::MAX
        );
    }
}
