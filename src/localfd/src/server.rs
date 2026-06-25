//! The storage-node side: a unix-domain socket that hands out volume descriptors.

use std::io::{IoSlice, Read};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
use soma_backend::LocalReader;

use crate::protocol::{frame, LocalReply, LocalRequest};

/// How long the accept loop sleeps between polls when idle / shutting down.
const POLL: Duration = Duration::from_millis(20);
/// Reject absurd request frames (a request is a few bytes).
const MAX_REQUEST: usize = 64 * 1024;

/// A running local-read server. Dropping it stops the accept loop and removes the
/// socket file.
pub struct LocalServer {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    path: PathBuf,
}

impl LocalServer {
    /// The bound socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Bind `path` and serve local reads from `reader` on a background thread. A stale
/// socket file at `path` is removed first; the parent directory is created if
/// missing.
pub fn serve_local_reads(
    path: impl Into<PathBuf>,
    reader: Arc<dyn LocalReader>,
) -> std::io::Result<LocalServer> {
    let path = path.into();
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(&path)?;
    listener.set_nonblocking(true)?;

    let stop = Arc::new(AtomicBool::new(false));
    let loop_stop = stop.clone();
    let join = std::thread::Builder::new()
        .name("soma-localfd".into())
        .spawn(move || accept_loop(listener, reader, loop_stop))?;

    Ok(LocalServer {
        stop,
        join: Some(join),
        path,
    })
}

fn accept_loop(listener: UnixListener, reader: Arc<dyn LocalReader>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let reader = reader.clone();
                let stop = stop.clone();
                // One thread per connection (co-located readers are few).
                let spawned = std::thread::Builder::new()
                    .name("soma-localfd-conn".into())
                    .spawn(move || serve_conn(stream, reader.as_ref(), &stop));
                if let Err(e) = spawned {
                    tracing::warn!(error = %e, "localfd: cannot spawn connection thread");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(POLL),
            Err(e) => {
                tracing::warn!(error = %e, "localfd accept failed");
                std::thread::sleep(POLL);
            }
        }
    }
}

fn serve_conn(mut stream: UnixStream, reader: &dyn LocalReader, stop: &AtomicBool) {
    // The accepted socket may be non-blocking; use blocking IO for framing.
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    while !stop.load(Ordering::Relaxed) {
        match handle_one(&mut stream, reader) {
            Ok(true) => {}       // served; keep the connection for the next request
            Ok(false) => break,  // clean EOF
            Err(e) => {
                tracing::debug!(error = %e, "localfd connection ended");
                break;
            }
        }
    }
}

/// Read and answer one request. `Ok(false)` signals a clean EOF.
fn handle_one(stream: &mut UnixStream, reader: &dyn LocalReader) -> std::io::Result<bool> {
    let mut len_buf = [0u8; 4];
    if !read_full_or_eof(stream, &mut len_buf)? {
        return Ok(false);
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_REQUEST {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "request frame too large",
        ));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;

    let req: LocalRequest = match postcard::from_bytes(&body) {
        Ok(r) => r,
        Err(_) => {
            send_reply(stream, &LocalReply::Error("malformed request".into()), None)?;
            return Ok(true);
        }
    };

    match reader.locate_fd(req.object_id) {
        Ok(needle) => {
            let reply = LocalReply::Ok {
                payload_offset: needle.payload_offset,
                len: needle.len,
                crc: needle.crc,
            };
            send_reply(stream, &reply, Some(needle.fd.as_raw_fd()))?;
            // `needle.fd` drops here — the kernel already duplicated it into the
            // socket buffer during sendmsg, so the receiver keeps a live reference.
        }
        Err(soma_backend::Error::ObjectNotFound(_)) => {
            send_reply(stream, &LocalReply::NotFound, None)?;
        }
        Err(e) => {
            send_reply(stream, &LocalReply::Error(e.to_string()), None)?;
        }
    }
    Ok(true)
}

/// Send a reply, attaching `fd` (if any) as `SCM_RIGHTS` ancillary data on the
/// same message as the reply bytes.
fn send_reply(
    stream: &mut UnixStream,
    reply: &LocalReply,
    fd: Option<std::os::fd::RawFd>,
) -> std::io::Result<()> {
    let framed =
        frame(reply).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let iov = [IoSlice::new(&framed)];
    let sock = stream.as_raw_fd();
    let res = match fd {
        Some(f) => {
            let fds = [f];
            let cmsgs = [ControlMessage::ScmRights(&fds)];
            sendmsg::<()>(sock, &iov, &cmsgs, MsgFlags::empty(), None)
        }
        None => sendmsg::<()>(sock, &iov, &[], MsgFlags::empty(), None),
    };
    res.map(|_| ()).map_err(std::io::Error::from)
}

/// Fill `buf` fully; `Ok(false)` if EOF arrives cleanly before any byte.
fn read_full_or_eof(stream: &mut UnixStream, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match stream.read(&mut buf[read..]) {
            Ok(0) => {
                return if read == 0 {
                    Ok(false)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "eof mid-frame",
                    ))
                }
            }
            Ok(n) => read += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}
