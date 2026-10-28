//! Required upstream SBE encoders for stdout, aligned writer for optional files.
use crate::{
    engine::Record,
    util::{AlignedSbeWriter, dtos::SbeEncode},
};
use anyhow::ensure;
use std::{
    io::{self, Write},
    path::Path,
};
pub const CHUNK_SIZE: usize = 64 << 20;
pub struct Output {
    file: Option<AlignedSbeWriter>,
    stdout: Option<io::BufWriter<io::Stdout>>,
    pub records: u64,
    poisoned: bool,
}
impl Output {
    pub fn new(path: Option<&Path>, stdout: bool) -> anyhow::Result<Self> {
        ensure!(
            stdout || path.is_some(),
            "enable stdout or provide --output"
        );
        // Reserve atomically so the upstream truncating constructor cannot overwrite user data.
        let file = if let Some(path) = path {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            Some(AlignedSbeWriter::create(path, CHUNK_SIZE)?)
        } else {
            None
        };
        Ok(Self {
            file,
            stdout: stdout.then(|| io::BufWriter::new(io::stdout())),
            records: 0,
            poisoned: false,
        })
    }
    // The supplied writer retains its chunk after a partial write error. Discard
    // it immediately: retrying that chunk could append after a partial prefix.
    fn file_operation(
        &mut self,
        operation: impl FnOnce(&mut AlignedSbeWriter) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if let Some(file) = &mut self.file
            && let Err(error) = operation(file)
        {
            self.poisoned = true;
            self.file.take();
            return Err(error);
        }
        Ok(())
    }
    fn poison_stdout(&mut self) {
        self.poisoned = true;
        if let Some(stdout) = self.stdout.take() {
            // into_parts deliberately skips BufWriter's implicit drop flush.
            let _ = stdout.into_parts();
        }
    }
    pub fn write(&mut self, record: &Record) -> anyhow::Result<()> {
        ensure!(
            !self.poisoned,
            "output is poisoned after an earlier I/O failure"
        );
        let msg: &dyn SbeEncode = match record {
            Record::Snapshot(snapshot) => snapshot,
            Record::Delta(delta) => delta,
        };
        self.file_operation(|file| file.write_msg(msg))?;
        if let Some(stdout) = &mut self.stdout
            && let Err(error) = stdout.write_all(&msg.to_bytes()?)
        {
            self.poison_stdout();
            return Err(error.into());
        }
        self.records += 1;
        Ok(())
    }
    pub fn flush_stdout(&mut self) -> anyhow::Result<()> {
        if let Some(stdout) = &mut self.stdout
            && let Err(error) = stdout.flush()
        {
            self.poison_stdout();
            return Err(error.into());
        }
        Ok(())
    }
    pub fn flush(&mut self) -> anyhow::Result<()> {
        let file = self.file_operation(AlignedSbeWriter::flush);
        let stdout = self.flush_stdout();
        file.and(stdout)?;
        ensure!(
            !self.poisoned,
            "output is poisoned after an earlier I/O failure"
        );
        Ok(())
    }
    pub fn finish(&mut self) -> anyhow::Result<()> {
        let result = self.flush();
        self.file.take();
        self.stdout.take();
        result
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        io::Read,
        os::fd::{AsRawFd, FromRawFd},
    };
    #[test]
    fn partial_aligned_write_error_is_never_retried_on_finish_or_drop() {
        let mut fds = [0; 2];
        // Own both descriptors immediately; no detached process or network listener.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let mut reader = unsafe { File::from_raw_fd(fds[0]) };
        let writer = unsafe { File::from_raw_fd(fds[1]) };
        for fd in [reader.as_raw_fd(), writer.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            assert!(flags >= 0);
            assert_eq!(
                unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
                0
            );
        }
        let path = format!("/dev/fd/{}", writer.as_raw_fd());
        let mut output = Output {
            file: Some(AlignedSbeWriter::create(path, CHUNK_SIZE).unwrap()),
            stdout: None,
            records: 0,
            poisoned: false,
        };
        let timestamp = chrono::DateTime::from_timestamp_millis(1000).unwrap();
        output
            .write(&Record::Snapshot(
                crate::util::dtos::challenge_orderbook::SnapshotDomain {
                    received_at: timestamp,
                    exchange_timestamp: timestamp,
                    bids: vec![],
                    asks: vec![],
                    symbol: "a".into(),
                },
            ))
            .unwrap();
        assert!(output.flush().is_err());
        fn drain(reader: &mut File) -> usize {
            let mut count = 0;
            let mut buffer = [0; 8192];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => count += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("{e}"),
                }
            }
            count
        }
        assert!(
            drain(&mut reader) > 0,
            "test requires a real partial write before error"
        );
        assert!(output.finish().is_err());
        drop(output);
        assert_eq!(
            drain(&mut reader),
            0,
            "failed chunk was retried and appended after its partial prefix"
        );
    }
}
