use std::{
    io,
    process::{Command, Output},
    time::Duration,
};

pub struct Limits {
    pub deadline: Duration,
    pub stream_bytes: usize,
}

pub async fn run(command: Command, limits: Limits) -> io::Result<Output> {
    #[cfg(target_os = "linux")]
    {
        linux::run(command, limits).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (command, limits);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "selected SDK child custody gates require Linux process groups",
        ))
    }
}

#[cfg(all(test, target_os = "linux"))]
pub fn assert_wait_retry_never_resignals() {
    linux::assert_wait_retry_never_resignals();
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use rustix::{
        fs::{OFlags, fcntl_getfl, fcntl_setfl},
        process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid},
    };
    use std::{
        io::Read,
        os::{fd::AsFd, unix::process::CommandExt},
        process::{Child, ChildStderr, ChildStdout, ExitStatus, Stdio},
        time::Instant,
    };

    const READS_PER_TURN: usize = 16;
    const READ_BYTES: usize = 8192;
    const POLL_INTERVAL: Duration = Duration::from_millis(5);

    enum CleanupState {
        FreshGroup,
        WaitOnly {
            signal_error: Option<rustix::io::Errno>,
        },
    }

    impl CleanupState {
        fn may_signal(&self) -> bool {
            matches!(self, Self::FreshGroup)
        }

        fn before_wait(&mut self, signal_error: Option<rustix::io::Errno>) {
            assert!(self.may_signal(), "group signal state is consumed once");
            *self = Self::WaitOnly { signal_error };
        }

        fn signal_error(&self) -> Option<rustix::io::Errno> {
            match self {
                Self::FreshGroup => None,
                Self::WaitOnly { signal_error } => *signal_error,
            }
        }
    }

    struct OwnedChild {
        child: Option<Child>,
        group: Pid,
        cleanup: CleanupState,
        stdout: ChildStdout,
        stderr: ChildStderr,
    }

    impl OwnedChild {
        fn spawn(mut command: Command) -> io::Result<Self> {
            command
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut child = command.spawn()?;
            let group =
                Pid::from_raw(child.id() as i32).expect("a spawned Linux child has a positive pid");
            let stdout = child.stdout.take().expect("stdout was piped");
            let stderr = child.stderr.take().expect("stderr was piped");
            let owned = Self {
                child: Some(child),
                group,
                cleanup: CleanupState::FreshGroup,
                stdout,
                stderr,
            };
            nonblocking(&owned.stdout)?;
            nonblocking(&owned.stderr)?;
            Ok(owned)
        }

        fn exited(&self) -> io::Result<bool> {
            let flags = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
            // Keep the leader unreaped until its process group has been signalled.
            Ok(waitid(WaitId::Pid(self.group), flags)?.is_some())
        }

        fn kill_and_reap(&mut self) -> io::Result<ExitStatus> {
            let child = self.child.as_mut().expect("original child was not reaped");
            if self.cleanup.may_signal() {
                let signal_error = match kill_process_group(self.group, Signal::KILL) {
                    Ok(()) | Err(rustix::io::Errno::SRCH) => None,
                    Err(error) => {
                        // This fallback also precedes every potentially reaping wait.
                        let _ = child.kill();
                        Some(error)
                    }
                };
                self.cleanup.before_wait(signal_error);
            }
            // A failed wait retains the original handle, but can never re-signal its pid/group.
            match child.wait() {
                Ok(status) => {
                    self.child = None;
                    if let Some(error) = self.cleanup.signal_error() {
                        return Err(io::Error::from(error));
                    }
                    Ok(status)
                }
                Err(error) => {
                    if let Some(signal_error) = self.cleanup.signal_error() {
                        eprintln!("SDK prior process-group signal failed: {signal_error}");
                    }
                    Err(error)
                }
            }
        }
    }

    impl Drop for OwnedChild {
        fn drop(&mut self) {
            if self.child.is_some()
                && let Err(error) = self.kill_and_reap()
            {
                eprintln!("SDK original-child cleanup failed; reap not certified: {error}");
            }
        }
    }

    #[cfg(test)]
    pub(super) fn assert_wait_retry_never_resignals() {
        for signal_error in [None, Some(rustix::io::Errno::PERM)] {
            let mut state = CleanupState::FreshGroup;
            assert!(state.may_signal());
            state.before_wait(signal_error);
            // Pure transition control, not an injected syscall or actual-reap witness.
            for _ in 0..3 {
                assert!(!state.may_signal());
                assert_eq!(state.signal_error(), signal_error);
            }
        }
    }

    fn nonblocking(fd: &impl AsFd) -> io::Result<()> {
        fcntl_setfl(fd, fcntl_getfl(fd)? | OFlags::NONBLOCK)?;
        Ok(())
    }

    fn drain(reader: &mut impl Read, bytes: &mut Vec<u8>, limit: usize) -> io::Result<bool> {
        let mut buffer = [0_u8; READ_BYTES];
        for _ in 0..READS_PER_TURN {
            match reader.read(&mut buffer) {
                Ok(0) => return Ok(true),
                Ok(count) => {
                    if count > limit.saturating_sub(bytes.len()) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "SDK child output exceeded its stream byte ceiling",
                        ));
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    pub(super) async fn run(command: Command, limits: Limits) -> io::Result<Output> {
        let deadline = Instant::now()
            .checked_add(limits.deadline)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid child deadline"))?;
        let mut owned = OwnedChild::spawn(command)?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut stdout_eof = false;
        let mut stderr_eof = false;
        let mut status = None;
        loop {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "SDK child exceeded its phase deadline",
                ));
            }
            if !stdout_eof {
                stdout_eof = drain(&mut owned.stdout, &mut stdout, limits.stream_bytes)?;
            }
            if !stderr_eof {
                stderr_eof = drain(&mut owned.stderr, &mut stderr, limits.stream_bytes)?;
            }
            if status.is_none() && owned.exited()? {
                // This also terminates same-group descendants holding the original pipes.
                status = Some(owned.kill_and_reap()?);
            }
            if let Some(status) = status
                && stdout_eof
                && stderr_eof
            {
                return Ok(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
