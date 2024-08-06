use crate::exfat;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ExfatMode {
    Rw,
    Ro,
    Any,
}

#[derive(Debug)]
pub(crate) enum ExfatRepair {
    Yes,
    No,
    Ask,
}

#[derive(Debug)]
pub(crate) enum ExfatNidAlloc {
    Linear,
    Bitmap,
}

#[derive(Debug)]
pub(crate) struct ExfatOption {
    pub(crate) mode: ExfatMode,
    pub(crate) repair: ExfatRepair,
    pub(crate) noatime: bool,
    pub(crate) dmask: exfat::ExfatStatMode,
    pub(crate) fmask: exfat::ExfatStatMode,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    pub(crate) nidalloc: ExfatNidAlloc,
    pub(crate) debug: bool,
}

impl ExfatOption {
    fn newopt() -> getopts::Options {
        let mut opts = getopts::Options::new();
        opts.optopt("", "mode", "", "<rw|ro|any>");
        opts.optopt("", "repair", "", "<yes|no|ask>");
        opts.optflag("", "noatime", "");
        opts.optopt("", "umask", "", "<octal_number>");
        opts.optopt("", "dmask", "", "<octal_number>");
        opts.optopt("", "fmask", "", "<octal_number>");
        opts.optopt("", "uid", "", "<number>");
        opts.optopt("", "gid", "", "<number>");
        opts.optopt("", "nidalloc", "", "<linear|bitmap>");
        opts.optflag("h", "help", "");
        opts.optflag("", "debug", "");
        opts
    }

    pub(crate) fn new(args: &[&str]) -> nix::Result<Self> {
        let opts = ExfatOption::newopt();
        let matches = match opts.parse(args) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return Err(nix::errno::Errno::EINVAL);
            }
        };
        if matches.opt_present("h") {
            println!("{}", opts.usage("exFAT options"));
            return Err(nix::errno::Errno::UnknownErrno); // 0
        }
        let mode = match matches.opt_str("mode") {
            Some(v) => match v.as_str() {
                "rw" => ExfatMode::Rw,
                "ro" => ExfatMode::Ro,
                "any" => ExfatMode::Any, // "ro_fallback" in relan/exfat
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => ExfatMode::Rw,
        };
        let repair = match matches.opt_str("repair") {
            Some(v) => match v.as_str() {
                "yes" => ExfatRepair::Yes,
                "no" => ExfatRepair::No,
                "ask" => ExfatRepair::Ask,
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => ExfatRepair::No,
        };
        let noatime = matches.opt_present("noatime");
        let umask = match matches.opt_str("umask") {
            Some(v) => match exfat::ExfatStatMode::from_str_radix(&v, 8) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => 0,
        };
        let dmask = match matches.opt_str("dmask") {
            Some(v) => match exfat::ExfatStatMode::from_str_radix(&v, 8) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{e}");
                    return Err(nix::errno::Errno::EINVAL);
                }
            },
            None => umask,
        };
        let fmask = match matches.opt_str("fmask") {
            Some(v) => match exfat::ExfatStatMode::from_str_radix(&v, 8) {
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
                "linear" => ExfatNidAlloc::Linear,
                "bitmap" => ExfatNidAlloc::Bitmap,
                _ => return Err(nix::errno::Errno::EINVAL),
            },
            None => ExfatNidAlloc::Linear,
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
        match super::ExfatOption::new(&["--mode", "rw"]) {
            Ok(v) => match v.mode {
                super::ExfatMode::Rw => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--mode", "ro"]) {
            Ok(v) => match v.mode {
                super::ExfatMode::Ro => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--mode", "any"]) {
            Ok(v) => match v.mode {
                super::ExfatMode::Any => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--mode", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_repair() {
        match super::ExfatOption::new(&["--repair", "yes"]) {
            Ok(v) => match v.repair {
                super::ExfatRepair::Yes => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--repair", "no"]) {
            Ok(v) => match v.repair {
                super::ExfatRepair::No => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--repair", "ask"]) {
            Ok(v) => match v.repair {
                super::ExfatRepair::Ask => (),
                v => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--repair", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_noatime() {
        match super::ExfatOption::new(&["--noatime"]) {
            Ok(v) => assert!(v.noatime),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert!(!v.noatime),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_dmask() {
        match super::ExfatOption::new(&["--dmask", "022"]) {
            Ok(v) => assert_eq!(v.dmask, 0o022),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--umask", "777"]) {
            Ok(v) => assert_eq!(v.dmask, 0o777),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert_eq!(v.dmask, 0),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_fmask() {
        match super::ExfatOption::new(&["--fmask", "644"]) {
            Ok(v) => assert_eq!(v.fmask, 0o644),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--umask", "777"]) {
            Ok(v) => assert_eq!(v.fmask, 0o777),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert_eq!(v.fmask, 0),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_uid() {
        match super::ExfatOption::new(&["--uid", "123"]) {
            Ok(v) => assert_eq!(v.uid, 123),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert_eq!(v.uid, nix::unistd::geteuid().as_raw()),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_gid() {
        match super::ExfatOption::new(&["--gid", "456"]) {
            Ok(v) => assert_eq!(v.gid, 456),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert_eq!(v.gid, nix::unistd::getegid().as_raw()),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_nidalloc() {
        match super::ExfatOption::new(&["--nidalloc", "linear"]) {
            Ok(v) => match v.nidalloc {
                super::ExfatNidAlloc::Linear => (),
                v @ super::ExfatNidAlloc::Bitmap => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--nidalloc", "bitmap"]) {
            Ok(v) => match v.nidalloc {
                super::ExfatNidAlloc::Bitmap => (),
                v @ super::ExfatNidAlloc::Linear => panic!("{v:?}"),
            },
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--nidalloc", "xxx"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::EINVAL) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_help() {
        match super::ExfatOption::new(&["-h"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::UnknownErrno) => (),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&["--h"]) {
            Ok(v) => panic!("{v:?}"),
            Err(nix::errno::Errno::UnknownErrno) => (),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn test_opt_debug() {
        match super::ExfatOption::new(&["--debug"]) {
            Ok(v) => assert!(v.debug),
            Err(e) => panic!("{e}"),
        }

        match super::ExfatOption::new(&[]) {
            Ok(v) => assert!(!v.debug),
            Err(e) => panic!("{e}"),
        }
    }
}
