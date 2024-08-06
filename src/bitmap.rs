use crate::util;

// size_t in relan/exfat, but u8 is easier to handle in Rust.
#[cfg(not(feature = "bitmap_u64"))]
pub(crate) type Bitmap = u8;
#[cfg(feature = "bitmap_u64")]
pub(crate) type Bitmap = u64;

const SIZE: usize = std::mem::size_of::<Bitmap>();
const SIZE_BITS: usize = SIZE * 8;

#[must_use]
pub fn alloc(count: usize) -> Vec<Bitmap> {
    vec![0; size(count) / SIZE]
}

#[must_use]
pub(crate) fn round_up(count: usize) -> usize {
    util::round_up!(count, SIZE_BITS)
}

#[must_use]
pub(crate) fn size(count: usize) -> usize {
    round_up(count) / 8 // bytes
}

pub(crate) fn block(index: usize) -> usize {
    index / SIZE_BITS // Bitmap array index
}

pub(crate) fn mask(index: usize) -> Bitmap {
    1 << (index % SIZE_BITS) // bit within a Bitmap
}

pub(crate) fn get(bitmap: &[Bitmap], index: usize) -> Bitmap {
    bitmap[block(index)] & mask(index)
}

pub fn set(bitmap: &mut [Bitmap], index: usize) {
    bitmap[block(index)] |= mask(index);
}

pub(crate) fn clear(bitmap: &mut [Bitmap], index: usize) {
    bitmap[block(index)] &= !mask(index);
}

pub(crate) fn find_and_set(bitmap: &mut [Bitmap], start: usize, end: usize) -> usize {
    let start_index = start / SIZE_BITS;
    let end_index = util::div_round_up!(end, SIZE_BITS); // not inclusive

    for i in start_index..end_index {
        if bitmap[i] == Bitmap::MAX {
            continue;
        }
        let start_bitindex = std::cmp::max(i * SIZE_BITS, start);
        let end_bitindex = std::cmp::min((i + 1) * SIZE_BITS, end);
        for c in start_bitindex..end_bitindex {
            if get(bitmap, c) == 0 {
                set(bitmap, c);
                return c;
            }
        }
    }
    usize::MAX
}

pub(crate) fn count(bitmap: &[Bitmap]) -> usize {
    let start = 0;
    let end = bitmap.len() * SIZE_BITS; // not inclusive
    let start_index = start / SIZE_BITS;
    let end_index = util::div_round_up!(end, SIZE_BITS);
    let mut total = 0;

    for i in start_index..end_index {
        if bitmap[i] == Bitmap::MAX {
            total += SIZE_BITS;
            continue;
        }
        if bitmap[i] == 0 {
            continue;
        }
        let start_bitindex = std::cmp::max(i * SIZE_BITS, start);
        let end_bitindex = std::cmp::min((i + 1) * SIZE_BITS, end);
        for c in start_bitindex..end_bitindex {
            if get(bitmap, c) != 0 {
                total += 1;
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_alloc() {
        let b = super::alloc(0);
        assert_eq!(b.len(), 0);

        let b = super::alloc(1);
        assert_eq!(b.len(), 1);
        let b = super::alloc(super::SIZE_BITS);
        assert_eq!(b.len(), 1);

        let b = super::alloc(super::SIZE_BITS + 1);
        assert_eq!(b.len(), 2);
        let b = super::alloc(super::SIZE_BITS * 2);
        assert_eq!(b.len(), 2);

        let b = super::alloc(super::SIZE_BITS * 2 + 1);
        assert_eq!(b.len(), 3);
        let b = super::alloc(super::SIZE_BITS * 3);
        assert_eq!(b.len(), 3);
    }

    #[test]
    fn test_round_up() {
        assert_eq!(super::round_up(0), 0);

        assert_eq!(super::round_up(1), super::SIZE_BITS);
        assert_eq!(super::round_up(super::SIZE_BITS), super::SIZE_BITS);

        assert_eq!(super::round_up(super::SIZE_BITS + 1), super::SIZE_BITS * 2);
        assert_eq!(super::round_up(super::SIZE_BITS * 2), super::SIZE_BITS * 2);

        assert_eq!(
            super::round_up(super::SIZE_BITS * 2 + 1),
            super::SIZE_BITS * 3
        );
        assert_eq!(super::round_up(super::SIZE_BITS * 3), super::SIZE_BITS * 3);
    }

    #[test]
    fn test_size() {
        assert_eq!(super::size(0), 0);

        assert_eq!(super::size(1), super::SIZE);
        assert_eq!(super::size(super::SIZE_BITS), super::SIZE);

        assert_eq!(super::size(super::SIZE_BITS + 1), super::SIZE * 2);
        assert_eq!(super::size(super::SIZE_BITS * 2), super::SIZE * 2);

        assert_eq!(super::size(super::SIZE_BITS * 2 + 1), super::SIZE * 3);
        assert_eq!(super::size(super::SIZE_BITS * 3), super::SIZE * 3);
    }

    #[test]
    fn test_block() {
        assert_eq!(super::block(0), 0);
        assert_eq!(super::block(super::SIZE_BITS - 1), 0);

        assert_eq!(super::block(super::SIZE_BITS), 1);
        assert_eq!(super::block(super::SIZE_BITS * 2 - 1), 1);

        assert_eq!(super::block(super::SIZE_BITS), 1);
        assert_eq!(super::block(super::SIZE_BITS * 2 - 1), 1);

        assert_eq!(super::block(super::SIZE_BITS * 2), 2);
        assert_eq!(super::block(super::SIZE_BITS * 3 - 1), 2);
    }

    #[test]
    fn test_mask() {
        assert_eq!(super::mask(0), 1);
        assert_eq!(super::mask(1), 2);
        assert_eq!(super::mask(2), 4);
        assert_eq!(super::mask(3), 8);
        assert_eq!(super::mask(4), 16);
        assert_eq!(super::mask(5), 32);
        assert_eq!(super::mask(6), 64);
        assert_eq!(super::mask(7), 128);

        #[cfg(feature = "bitmap_u64")]
        {
            assert_eq!(super::mask(8), 1 << 8);
            assert_eq!(super::mask(9), 1 << 9);
            assert_eq!(super::mask(10), 1 << 10);
            assert_eq!(super::mask(11), 1 << 11);
            assert_eq!(super::mask(12), 1 << 12);
            assert_eq!(super::mask(13), 1 << 13);
            assert_eq!(super::mask(14), 1 << 14);
            assert_eq!(super::mask(15), 1 << 15);
        }

        assert_eq!(super::mask(super::SIZE_BITS), 1);
        assert_eq!(super::mask(super::SIZE_BITS + 1), 2);
        assert_eq!(super::mask(super::SIZE_BITS + 2), 4);
        assert_eq!(super::mask(super::SIZE_BITS + 3), 8);
        assert_eq!(super::mask(super::SIZE_BITS + 4), 16);
        assert_eq!(super::mask(super::SIZE_BITS + 5), 32);
        assert_eq!(super::mask(super::SIZE_BITS + 6), 64);
        assert_eq!(super::mask(super::SIZE_BITS + 7), 128);

        #[cfg(feature = "bitmap_u64")]
        {
            assert_eq!(super::mask(super::SIZE_BITS + 8), 1 << 8);
            assert_eq!(super::mask(super::SIZE_BITS + 9), 1 << 9);
            assert_eq!(super::mask(super::SIZE_BITS + 10), 1 << 10);
            assert_eq!(super::mask(super::SIZE_BITS + 11), 1 << 11);
            assert_eq!(super::mask(super::SIZE_BITS + 12), 1 << 12);
            assert_eq!(super::mask(super::SIZE_BITS + 13), 1 << 13);
            assert_eq!(super::mask(super::SIZE_BITS + 14), 1 << 14);
            assert_eq!(super::mask(super::SIZE_BITS + 15), 1 << 15);
        }
    }

    #[test]
    fn test_get() {
        let b = [0];
        assert_eq!(super::get(&b, 0), 0);
        assert_eq!(super::get(&b, 1), 0);
        assert_eq!(super::get(&b, 2), 0);
        let b = [1];
        assert_eq!(super::get(&b, 0), 1);
        assert_eq!(super::get(&b, 1), 0);
        assert_eq!(super::get(&b, 2), 0);
        let b = [2];
        assert_eq!(super::get(&b, 0), 0);
        assert_eq!(super::get(&b, 1), 2);
        assert_eq!(super::get(&b, 2), 0);
        let b = [3];
        assert_eq!(super::get(&b, 0), 1);
        assert_eq!(super::get(&b, 1), 2);
        assert_eq!(super::get(&b, 2), 0);
        let b = [4];
        assert_eq!(super::get(&b, 0), 0);
        assert_eq!(super::get(&b, 1), 0);
        assert_eq!(super::get(&b, 2), 4);
        let b = [5];
        assert_eq!(super::get(&b, 0), 1);
        assert_eq!(super::get(&b, 1), 0);
        assert_eq!(super::get(&b, 2), 4);
        let b = [6];
        assert_eq!(super::get(&b, 0), 0);
        assert_eq!(super::get(&b, 1), 2);
        assert_eq!(super::get(&b, 2), 4);
        let b = [7];
        assert_eq!(super::get(&b, 0), 1);
        assert_eq!(super::get(&b, 1), 2);
        assert_eq!(super::get(&b, 2), 4);

        let b = [0, 0];
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let b = [0, 1];
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let b = [0, 2];
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let b = [0, 3];
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let b = [0, 4];
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let b = [0, 5];
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let b = [0, 6];
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let b = [0, 7];
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
    }

    #[test]
    fn test_set() {
        let mut b = [0];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [1];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [2];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [3];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [4];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [5];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [6];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);
        let mut b = [7];
        super::set(&mut b, 0);
        assert_eq!(super::get(&b, 0), 1);
        super::set(&mut b, 1);
        assert_eq!(super::get(&b, 1), 2);
        super::set(&mut b, 2);
        assert_eq!(super::get(&b, 2), 4);

        let mut b = [0, 0];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 1];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 2];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 3];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 4];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 5];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 6];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
        let mut b = [0, 7];
        super::set(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 1);
        super::set(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 2);
        super::set(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 4);
    }

    #[test]
    fn test_clear() {
        let mut b = [0];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [1];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [2];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [3];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [4];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [5];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [6];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);
        let mut b = [7];
        super::clear(&mut b, 0);
        assert_eq!(super::get(&b, 0), 0);
        super::clear(&mut b, 1);
        assert_eq!(super::get(&b, 1), 0);
        super::clear(&mut b, 2);
        assert_eq!(super::get(&b, 2), 0);

        let mut b = [0, 0];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 1];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 2];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 3];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 4];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 5];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 6];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
        let mut b = [0, 7];
        super::clear(&mut b, super::SIZE_BITS);
        assert_eq!(super::get(&b, super::SIZE_BITS), 0);
        super::clear(&mut b, super::SIZE_BITS + 1);
        assert_eq!(super::get(&b, super::SIZE_BITS + 1), 0);
        super::clear(&mut b, super::SIZE_BITS + 2);
        assert_eq!(super::get(&b, super::SIZE_BITS + 2), 0);
    }

    #[test]
    fn test_find_and_set() {
        let mut b = [0];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 2);
        let mut b = [1];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        let mut b = [2];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        let mut b = [3];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 2);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 4);
        let mut b = [4];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        let mut b = [5];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 4);
        let mut b = [6];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 4);
        let mut b = [7];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 3);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 4);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 5);
        let mut b = [super::Bitmap::MAX];
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        let mut b = [super::Bitmap::MAX - 1];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        let mut b = [super::Bitmap::MAX - 2];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);
        let mut b = [super::Bitmap::MAX - 3];
        assert_eq!(super::find_and_set(&mut b, 0, 8), 0);
        assert_eq!(super::find_and_set(&mut b, 0, 8), 1);
        assert_eq!(super::find_and_set(&mut b, 0, 8), usize::MAX);

        let mut b = [super::Bitmap::MAX, 0];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 2
        );
        let mut b = [super::Bitmap::MAX, 1];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 2
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        let mut b = [super::Bitmap::MAX, 2];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 2
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        let mut b = [super::Bitmap::MAX, 3];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 2
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 4
        );
        let mut b = [super::Bitmap::MAX, 4];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        let mut b = [super::Bitmap::MAX, 5];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 4
        );
        let mut b = [super::Bitmap::MAX, 6];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 4
        );
        let mut b = [super::Bitmap::MAX, 7];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 3
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 4
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 5
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 1];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 2];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
        let mut b = [super::Bitmap::MAX, super::Bitmap::MAX - 3];
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            super::SIZE_BITS + 1
        );
        assert_eq!(
            super::find_and_set(&mut b, 0, super::SIZE_BITS * 2),
            usize::MAX
        );
    }

    #[test]
    fn test_count() {
        let mut b = [0, 0];
        assert_eq!(super::count(&b), 0);

        super::set(&mut b, 0);
        assert_eq!(super::count(&b), 1);
        super::set(&mut b, 1);
        assert_eq!(super::count(&b), 2);
        super::set(&mut b, 2);
        assert_eq!(super::count(&b), 3);
        super::set(&mut b, 3);
        assert_eq!(super::count(&b), 4);
        super::set(&mut b, 4);
        assert_eq!(super::count(&b), 5);
        super::set(&mut b, 5);
        assert_eq!(super::count(&b), 6);
        super::set(&mut b, 6);
        assert_eq!(super::count(&b), 7);
        super::set(&mut b, 7);
        assert_eq!(super::count(&b), 8);
        super::set(&mut b, 8);
        assert_eq!(super::count(&b), 9);
        super::set(&mut b, 9);
        assert_eq!(super::count(&b), 10);
        super::set(&mut b, 10);
        assert_eq!(super::count(&b), 11);
        super::set(&mut b, 11);
        assert_eq!(super::count(&b), 12);
        super::set(&mut b, 12);
        assert_eq!(super::count(&b), 13);
        super::set(&mut b, 13);
        assert_eq!(super::count(&b), 14);
        super::set(&mut b, 14);
        assert_eq!(super::count(&b), 15);
        super::set(&mut b, 15);
        assert_eq!(super::count(&b), 16);

        super::clear(&mut b, 15);
        assert_eq!(super::count(&b), 15);
        super::clear(&mut b, 14);
        assert_eq!(super::count(&b), 14);
        super::clear(&mut b, 13);
        assert_eq!(super::count(&b), 13);
        super::clear(&mut b, 12);
        assert_eq!(super::count(&b), 12);
        super::clear(&mut b, 11);
        assert_eq!(super::count(&b), 11);
        super::clear(&mut b, 10);
        assert_eq!(super::count(&b), 10);
        super::clear(&mut b, 9);
        assert_eq!(super::count(&b), 9);
        super::clear(&mut b, 8);
        assert_eq!(super::count(&b), 8);
        super::clear(&mut b, 7);
        assert_eq!(super::count(&b), 7);
        super::clear(&mut b, 6);
        assert_eq!(super::count(&b), 6);
        super::clear(&mut b, 5);
        assert_eq!(super::count(&b), 5);
        super::clear(&mut b, 4);
        assert_eq!(super::count(&b), 4);
        super::clear(&mut b, 3);
        assert_eq!(super::count(&b), 3);
        super::clear(&mut b, 2);
        assert_eq!(super::count(&b), 2);
        super::clear(&mut b, 1);
        assert_eq!(super::count(&b), 1);
        super::clear(&mut b, 0);
        assert_eq!(super::count(&b), 0);

        assert_eq!(b[0], 0);
        assert_eq!(b[1], 0);

        b[0] = 0xf;
        assert_eq!(super::count(&b), 4);

        b[1] = 0xf;
        assert_eq!(super::count(&b), 8);
    }
}
