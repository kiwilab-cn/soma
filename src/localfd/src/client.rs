//! The reader side: connect to a node's local socket and obtain a descriptor.

use std::io::{IoSliceMut, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;

use nix::cmsg_space;
use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};

use crate::protocol::{frame, LocalReply, LocalRequest};
use crate::{Error, Result};

/// A located object, as an open descriptor plus payload framing. Read the payload
/// from `fd` at `[payload_offset, payload_offset + len)` (e.g. `pread` or `mmap`)
/// and verify it against `crc`.
#[derive(Debug)]
pub struct LocalRead {
    /// Descriptor for the volume file holding the object.
    pub fd: OwnedFd,
    /// Byte offset of the payload within `fd`.
    pub payload_offset: u64,
    /// Payload length in bytes.
    pub len: u32,
    /// CRC32C the caller must verify the payload against.
    pub crc: u32,
}

/// A connection to a storage node's local-read socket. Reusable for many reads.
#[derive(Debug)]
pub struct LocalClient {
    stream: UnixStream,
}

impl LocalClient {
    /// Connect to the node's local-read socket.
    pub fn connect(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            stream: UnixStream::connect(path)?,
        })
    }

    /// Resolve `object_id` to a descriptor + framing over the local socket. The
    /// descriptor references the same kernel open file as the storage node's —
    /// no bytes cross the socket, only the descriptor.
    pub fn read_fd(&mut self, object_id: u64) -> Result<LocalRead> {
        let req = frame(&LocalRequest { object_id })?;
        self.stream.write_all(&req)?;

        let mut buf = [0u8; 512];
        let mut iov = [IoSliceMut::new(&mut buf)];
        let mut cmsg = cmsg_space!([RawFd; 1]);
        let msg = recvmsg::<()>(
            self.stream.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg),
            MsgFlags::empty(),
        )
        .map_err(std::io::Error::from)?;

        // Take any descriptor first, so it is owned (and closed on any later error).
        let mut recv_fd: Option<OwnedFd> = None;
        for c in msg.cmsgs().map_err(std::io::Error::from)? {
            if let ControlMessageOwned::ScmRights(fds) = c {
                for raw in fds {
                    // Safety: the kernel just handed us this fd; we own it.
                    recv_fd = Some(unsafe { OwnedFd::from_raw_fd(raw) });
                }
            }
        }

        let n = msg.bytes;
        if n < 4 {
            return Err(Error::Closed);
        }
        let body_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if 4 + body_len > n {
            return Err(Error::Malformed("reply body truncated"));
        }
        let reply: LocalReply = postcard::from_bytes(&buf[4..4 + body_len])?;
        match reply {
            LocalReply::Ok {
                payload_offset,
                len,
                crc,
            } => {
                let fd = recv_fd.ok_or(Error::Malformed("ok reply without descriptor"))?;
                Ok(LocalRead {
                    fd,
                    payload_offset,
                    len,
                    crc,
                })
            }
            LocalReply::NotFound => Err(Error::NotFound),
            LocalReply::Error(m) => Err(Error::Remote(m)),
        }
    }
}
