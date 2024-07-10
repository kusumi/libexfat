fn utf16_to_wchar(input: &[u16], wc: &mut u64, insize: usize) -> isize {
    if (u16::from_le(input[0]) & 0xfc00) == 0xd800 {
        if insize < 2 || (u16::from_le(input[1]) & 0xfc00) != 0xdc00 {
            return -1;
        }
        *wc = u64::from(u16::from_le(input[0]) & 0x3ff) << 10;
        *wc |= u64::from(u16::from_le(input[1]) & 0x3ff);
        *wc += 0x10000;
        2
    } else {
        *wc = u16::from_le(input[0]).into();
        1
    }
}

fn wchar_to_utf8(output: &mut [u8], wc: u64, outsize: usize) -> isize {
    if wc <= 0x7f {
        if outsize < 1 {
            return -1;
        }
        output[0] = wc.try_into().unwrap();
        1
    } else if wc <= 0x7ff {
        if outsize < 2 {
            return -1;
        }
        output[0] = u8::try_from(0xc0 | (wc >> 6)).unwrap();
        output[1] = u8::try_from(0x80 | (wc & 0x3f)).unwrap();
        2
    } else if wc <= 0xffff {
        if outsize < 3 {
            return -1;
        }
        output[0] = u8::try_from(0xe0 | (wc >> 12)).unwrap();
        output[1] = u8::try_from(0x80 | ((wc >> 6) & 0x3f)).unwrap();
        output[2] = u8::try_from(0x80 | (wc & 0x3f)).unwrap();
        3
    } else if wc <= 0x001f_ffff {
        if outsize < 4 {
            return -1;
        }
        output[0] = u8::try_from(0xf0 | (wc >> 18)).unwrap();
        output[1] = u8::try_from(0x80 | ((wc >> 12) & 0x3f)).unwrap();
        output[2] = u8::try_from(0x80 | ((wc >> 6) & 0x3f)).unwrap();
        output[3] = u8::try_from(0x80 | (wc & 0x3f)).unwrap();
        4
    } else if wc <= 0x03ff_ffff {
        if outsize < 5 {
            return -1;
        }
        output[0] = u8::try_from(0xf8 | (wc >> 24)).unwrap();
        output[1] = u8::try_from(0x80 | ((wc >> 18) & 0x3f)).unwrap();
        output[2] = u8::try_from(0x80 | ((wc >> 12) & 0x3f)).unwrap();
        output[3] = u8::try_from(0x80 | ((wc >> 6) & 0x3f)).unwrap();
        output[4] = u8::try_from(0x80 | (wc & 0x3f)).unwrap();
        5
    } else if wc <= 0x7fff_ffff {
        if outsize < 6 {
            return -1;
        }
        output[0] = u8::try_from(0xfc | (wc >> 30)).unwrap();
        output[1] = u8::try_from(0x80 | ((wc >> 24) & 0x3f)).unwrap();
        output[2] = u8::try_from(0x80 | ((wc >> 18) & 0x3f)).unwrap();
        output[3] = u8::try_from(0x80 | ((wc >> 12) & 0x3f)).unwrap();
        output[4] = u8::try_from(0x80 | ((wc >> 6) & 0x3f)).unwrap();
        output[5] = u8::try_from(0x80 | (wc & 0x3f)).unwrap();
        6
    } else {
        -1
    }
}

pub fn utf16_to_utf8(input: &[u16], outsize: usize, insize: usize) -> nix::Result<Vec<u8>> {
    let mut output = vec![0; outsize];
    let mut iptr = 0;
    let mut optr = 0;
    let mut wc = 0;

    while iptr < insize {
        let x = utf16_to_wchar(&input[iptr..], &mut wc, insize - iptr);
        if x < 0 {
            log::error!("illegal UTF-16 sequence");
            return Err(nix::errno::Errno::EILSEQ);
        }
        iptr += usize::try_from(x).unwrap();
        let x = wchar_to_utf8(&mut output[optr..], wc, outsize - optr);
        if x < 0 {
            log::error!("name is too long");
            return Err(nix::errno::Errno::ENAMETOOLONG);
        }
        optr += usize::try_from(x).unwrap();
        if wc == 0 {
            return Ok(output);
        }
    }

    match optr.cmp(&outsize) {
        std::cmp::Ordering::Greater => {
            log::error!("optr > outsize");
            return Err(nix::errno::Errno::ENAMETOOLONG);
        }
        std::cmp::Ordering::Less => {
            output[optr] = 0;
        }
        std::cmp::Ordering::Equal => (),
    }
    Ok(output)
}

fn utf8_to_wchar(input: &[u8], wc: &mut u64, insize: usize) -> isize {
    assert_ne!(insize, 0, "no input for utf8_to_wchar");

    let size = if (input[0] & 0x80) == 0 {
        *wc = input[0].into();
        return 1;
    } else if (input[0] & 0xe0) == 0xc0 {
        *wc = u64::from(input[0] & 0x1f) << 6;
        2
    } else if (input[0] & 0xf0) == 0xe0 {
        *wc = u64::from(input[0] & 0x0f) << 12;
        3
    } else if (input[0] & 0xf8) == 0xf0 {
        *wc = u64::from(input[0] & 0x07) << 18;
        4
    } else if (input[0] & 0xfc) == 0xf8 {
        *wc = u64::from(input[0] & 0x03) << 24;
        5
    } else if (input[0] & 0xfe) == 0xfc {
        *wc = u64::from(input[0] & 0x01) << 30;
        6
    } else {
        return -1;
    };

    if insize < size {
        return -1;
    }

    // the first byte is handled above
    for (i, x) in input.iter().enumerate().take(size).skip(1) {
        if (x & 0xc0) != 0x80 {
            return -1;
        }
        *wc |= u64::from(x & 0x3f) << ((size - i - 1) * 6);
    }
    size.try_into().unwrap()
}

fn wchar_to_utf16(output: &mut [u16], wc: u64, outsize: usize) -> isize {
    // if character is from BMP
    if wc <= 0xffff {
        if outsize == 0 {
            return -1;
        }
        output[0] = u16::try_from(wc).unwrap().to_le();
        return 1;
    }

    if outsize < 2 {
        return -1;
    }

    let mut wc = wc;
    wc -= 0x10000;
    output[0] = u16::try_from(0xd800 | ((wc >> 10) & 0x3ff))
        .unwrap()
        .to_le();
    output[1] = u16::try_from(0xdc00 | (wc & 0x3ff)).unwrap().to_le();
    2
}

pub fn utf8_to_utf16(input: &[u8], outsize: usize, insize: usize) -> nix::Result<Vec<u16>> {
    let mut output = vec![0; outsize];
    let mut iptr = 0;
    let mut optr = 0;
    let mut wc = 0;

    while iptr < insize {
        let x = utf8_to_wchar(&input[iptr..], &mut wc, insize - iptr);
        if x < 0 {
            log::error!("illegal UTF-8 sequence");
            return Err(nix::errno::Errno::EILSEQ);
        }
        iptr += usize::try_from(x).unwrap();
        let x = wchar_to_utf16(&mut output[optr..], wc, outsize - optr);
        if x < 0 {
            log::error!("name is too long");
            return Err(nix::errno::Errno::ENAMETOOLONG);
        }
        optr += usize::try_from(x).unwrap();
        if wc == 0 {
            break;
        }
    }

    match optr.cmp(&outsize) {
        std::cmp::Ordering::Greater => {
            log::error!("optr > outsize");
            return Err(nix::errno::Errno::ENAMETOOLONG);
        }
        std::cmp::Ordering::Less => {
            output[optr] = 0;
        }
        std::cmp::Ordering::Equal => (),
    }
    Ok(output)
}

// relan/exfat assumes str ends with \0
#[must_use]
pub fn utf16_length(str: &[u16]) -> usize {
    let mut i = 0;
    for x in str {
        if u16::from_le(*x) == 0 {
            break;
        }
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_utf16_to_utf8() {
        let mut input = vec![];
        for i in 1..=127 {
            input.push(i);
        }
        assert_eq!(input.len(), 127);

        let output = match super::utf16_to_utf8(&input, input.len(), input.len()) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        for i in 1..=127 {
            assert_eq!(output[i - 1], i.try_into().unwrap());
        }
    }

    #[test]
    fn test_utf16_to_utf8_string() {
        let input = vec![101, 120, 70, 65, 84];
        assert_eq!(input.len(), 5);

        let output = match super::utf16_to_utf8(&input, input.len(), input.len()) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(std::str::from_utf8(&output), Ok("exFAT"));
    }

    #[test]
    fn test_utf8_to_utf16() {
        let mut input = vec![];
        for i in 1..=127 {
            input.push(i);
        }
        assert_eq!(input.len(), 127);

        let output = match super::utf8_to_utf16(&input, input.len(), input.len()) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        for i in 1..=127 {
            assert_eq!(output[i - 1], i.try_into().unwrap());
        }
    }

    #[test]
    fn test_utf8_to_utf16_string() {
        let input = vec![101, 120, 70, 65, 84];
        assert_eq!(input.len(), 5);

        let output = match super::utf8_to_utf16(&input, input.len(), input.len()) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        let mut b = vec![];
        for x in &output {
            b.push((*x).try_into().unwrap());
        }
        assert_eq!(std::str::from_utf8(&b), Ok("exFAT"));
    }

    #[test]
    fn test_utf16_length() {
        assert_eq!(super::utf16_length(&[0]), 0);
        assert_eq!(super::utf16_length(&[0, 0]), 0);
        assert_eq!(super::utf16_length(&[0, 65]), 0);

        assert_eq!(super::utf16_length(&[65]), 1);
        assert_eq!(super::utf16_length(&[65, 0]), 1);
        assert_eq!(super::utf16_length(&[65, 0, 66]), 1);

        assert_eq!(super::utf16_length(&[65, 66]), 2);
        assert_eq!(super::utf16_length(&[65, 66, 0]), 2);
        assert_eq!(super::utf16_length(&[65, 66, 0, 67]), 2);
    }
}
