#[derive(Clone, Copy, Debug)]
pub(crate) enum OpenMode {
    Rw,
    Ro,
    Any,
}

#[derive(Debug)]
pub(crate) enum RepairMode {
    Yes,
    No,
    Ask,
}

#[derive(Debug)]
pub(crate) enum NidAllocMode {
    Linear,
    Bitmap,
}

#[derive(Debug)]
pub(crate) struct Opt {
    pub(crate) mode: OpenMode,
    pub(crate) repair: RepairMode,
    pub(crate) noatime: bool,
    pub(crate) dmask: crate::exfat::StatMode,
    pub(crate) fmask: crate::exfat::StatMode,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    pub(crate) nidalloc: NidAllocMode,
    pub(crate) debug: bool,
}

impl Opt {
    fn newopt() -> getopts::Options {
        let mut gopt = getopts::Options::new();
        gopt.optopt("", "mode", "", "<rw|ro|any>");
        gopt.optopt("", "repair", "", "<yes|no|ask>");
        gopt.optflag("", "noatime", "");
        gopt.optopt("", "umask", "", "<octal_number>");
        gopt.optopt("", "dmask", "", "<octal_number>");
        gopt.optopt("", "fmask", "", "<octal_number>");
        gopt.optopt("", "uid", "", "<number>");
        gopt.optopt("", "gid", "", "<number>");
        gopt.optopt("", "nidalloc", "", "<linear|bitmap>");
        gopt.optflag("h", "help", "");
        gopt.optflag("", "debug", "");
        gopt
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn new(args: &[&str]) -> nix::Result<Self> {
        let gopt = Opt::newopt();
        let matches = match gopt.parse(args) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return Err(nix::errno::Errno::EINVAL);
            }
        };
        if matches.opt_present("h") {
            println!("{}", gopt.usage("exFAT options"));
            return Err(nix::errno::Errno::UnknownErrno); // 0
        }
        let mode = match matches.opt_str("mode") {
            Some(v) => match v.as_str() {
                "rw" => OpenMode::Rw,
                "ro" => OpenMode::Ro,
                "any" => OpenMode::Any, // "ro_fallback" in relan/exfat
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => OpenMode::Rw,
        };
        let repair = match matches.opt_str("repair") {
            Some(v) => match v.as_str() {
                "yes" => RepairMode::Yes,
                "no" => RepairMode::No,
                "ask" => RepairMode::Ask,
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => RepairMode::No,
        };
        let noatime = matches.opt_present("noatime");
        let umask = match matches.opt_str("umask") {
            Some(v) => match crate::exfat::StatMode::from_str_radix(&v, 8) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => 0,
        };
        let dmask = match matches.opt_str("dmask") {
            Some(v) => match crate::exfat::StatMode::from_str_radix(&v, 8) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => umask,
        };
        let fmask = match matches.opt_str("fmask") {
            Some(v) => match crate::exfat::StatMode::from_str_radix(&v, 8) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => umask,
        };
        let uid = match matches.opt_str("uid") {
            Some(v) => match v.parse() {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => nix::unistd::geteuid().as_raw(),
        };
        let gid = match matches.opt_str("gid") {
            Some(v) => match v.parse() {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => nix::unistd::getegid().as_raw(),
        };
        let nidalloc = match matches.opt_str("nidalloc") {
            Some(v) => match v.as_str() {
                "linear" => NidAllocMode::Linear,
                "bitmap" => NidAllocMode::Bitmap,
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => NidAllocMode::Linear,
        };

        let debug = matches.opt_present("debug");
        Ok(Self {
            mode,
            repair,
            noatime,
            dmask,
            fmask,
            uid,
            gid,
            nidalloc,
            debug,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_opt_mode() {
        match super::Opt::new(&["--mode", "rw"]) {
            Ok(v) => match v.mode {
                super::OpenMode::Rw => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--mode", "ro"]) {
            Ok(v) => match v.mode {
                super::OpenMode::Ro => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--mode", "any"]) {
            Ok(v) => match v.mode {
                super::OpenMode::Any => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--mode", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_repair() {
        match super::Opt::new(&["--repair", "yes"]) {
            Ok(v) => match v.repair {
                super::RepairMode::Yes => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--repair", "no"]) {
            Ok(v) => match v.repair {
                super::RepairMode::No => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--repair", "ask"]) {
            Ok(v) => match v.repair {
                super::RepairMode::Ask => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--repair", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_noatime() {
        match super::Opt::new(&["--noatime"]) {
            Ok(v) => assert!(v.noatime),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert!(!v.noatime),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_dmask() {
        match super::Opt::new(&["--dmask", "022"]) {
            Ok(v) => assert_eq!(v.dmask, 0o022),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--umask", "777"]) {
            Ok(v) => assert_eq!(v.dmask, 0o777),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert_eq!(v.dmask, 0),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_fmask() {
        match super::Opt::new(&["--fmask", "644"]) {
            Ok(v) => assert_eq!(v.fmask, 0o644),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--umask", "777"]) {
            Ok(v) => assert_eq!(v.fmask, 0o777),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert_eq!(v.fmask, 0),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_uid() {
        match super::Opt::new(&["--uid", "123"]) {
            Ok(v) => assert_eq!(v.uid, 123),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert_eq!(v.uid, nix::unistd::geteuid().as_raw()),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_gid() {
        match super::Opt::new(&["--gid", "456"]) {
            Ok(v) => assert_eq!(v.gid, 456),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert_eq!(v.gid, nix::unistd::getegid().as_raw()),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_nidalloc() {
        match super::Opt::new(&["--nidalloc", "linear"]) {
            Ok(v) => match v.nidalloc {
                super::NidAllocMode::Linear => (),
                v @ super::NidAllocMode::Bitmap => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--nidalloc", "bitmap"]) {
            Ok(v) => match v.nidalloc {
                super::NidAllocMode::Bitmap => (),
                v @ super::NidAllocMode::Linear => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--nidalloc", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_help() {
        match super::Opt::new(&["-h"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::UnknownErrno) => (),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&["--h"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::UnknownErrno) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_debug() {
        match super::Opt::new(&["--debug"]) {
            Ok(v) => assert!(v.debug),
            Err(e) => panic!("{e}"),
        }

        match super::Opt::new(&[]) {
            Ok(v) => assert!(!v.debug),
            Err(e) => panic!("{e}"),
        }
    }
}
