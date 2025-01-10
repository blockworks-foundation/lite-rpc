use ring::rand::SecureRandom;

pub fn validate_token<'a>(
    src: &std::net::SocketAddr,
    token: &'a [u8],
) -> Option<quiche::ConnectionId<'a>> {
    if token.len() < 6 {
        return None;
    }

    if &token[..6] != b"quiche" {
        return None;
    }

    let token = &token[6..];

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    if token.len() < addr.len() || &token[..addr.len()] != addr.as_slice() {
        return None;
    }

    Some(quiche::ConnectionId::from_ref(&token[addr.len()..]))
}

pub fn mint_token(hdr: &quiche::Header, src: &std::net::SocketAddr) -> Vec<u8> {
    let mut token = Vec::new();

    token.extend_from_slice(b"quiche");

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    token.extend_from_slice(&addr);
    token.extend_from_slice(&hdr.dcid);

    token
}

pub fn is_bidi(stream_id: u64) -> bool {
    (stream_id & 0x2) == 0
}

pub fn get_next_bidi(mut current_stream_id: u64, max_number_of_streams: u64) -> u64 {
    loop {
        for stream_id in current_stream_id + 1.. {
            if stream_id >= max_number_of_streams {
                break;
            }
            if is_bidi(stream_id) {
                return stream_id;
            }
        }
        current_stream_id = 0;
    }
}

pub fn is_unidi(stream_id: u64, is_server: bool) -> bool {
    (stream_id & 0x1) == (is_server as u64)
}

pub fn get_next_unidi(
    mut current_stream_id: u64,
    is_server: bool,
    max_number_of_streams: u64,
) -> u64 {
    loop {
        for stream_id in current_stream_id + 1.. {
            if stream_id >= max_number_of_streams {
                break;
            }

            if is_unidi(stream_id, is_server) && !is_bidi(stream_id) {
                return stream_id;
            }
        }
        current_stream_id = 0;
    }
}

pub fn handle_path_events(conn: &mut quiche::Connection) {
    while let Some(qe) = conn.path_event_next() {
        match qe {
            quiche::PathEvent::New(local_addr, peer_addr) => {
                log::info!(
                    "{} Seen new path ({}, {})",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );

                // Directly probe the new path.
                conn.probe_path(local_addr, peer_addr)
                    .expect("cannot probe");
            }

            quiche::PathEvent::Validated(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now validated",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::FailedValidation(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) failed validation",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::Closed(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now closed and unusable",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::ReusedSourceConnectionId(cid_seq, old, new) => {
                log::info!(
                    "{} Peer reused cid seq {} (initially {:?}) on {:?}",
                    conn.trace_id(),
                    cid_seq,
                    old,
                    new
                );
            }

            quiche::PathEvent::PeerMigrated(local_addr, peer_addr) => {
                log::info!(
                    "{} Connection migrated to ({}, {})",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }
        }
    }
}

pub fn generate_cid_and_reset_token<T: SecureRandom>(
    rng: &T,
) -> (quiche::ConnectionId<'static>, u128) {
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    rng.fill(&mut scid).unwrap();
    let scid = scid.to_vec().into();
    let mut reset_token = [0; 16];
    rng.fill(&mut reset_token).unwrap();
    let reset_token = u128::from_be_bytes(reset_token);
    (scid, reset_token)
}

#[cfg(target_os = "linux")]
pub fn detect_gso(socket: &mio::net::UdpSocket, segment_size: usize) -> bool {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::UdpGsoSegment;
    use std::os::unix::io::AsRawFd;

    // mio::net::UdpSocket doesn't implement AsFd (yet?).
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket.as_raw_fd()) };

    setsockopt(&fd, UdpGsoSegment, &(segment_size as i32)).is_ok()
}

#[cfg(not(target_os = "linux"))]
pub fn detect_gso(_: &mio::net::UdpSocket, _: usize) -> bool {
    false
}

/// Set SO_TXTIME socket option.
///
/// This socket option is set to send to kernel the outgoing UDP
/// packet transmission time in the sendmsg syscall.
///
/// Note that this socket option is set only on linux platforms.
#[cfg(target_os = "linux")]
pub fn set_txtime_sockopt(sock: &mio::net::UdpSocket) -> std::io::Result<()> {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::TxTime;
    use std::os::unix::io::AsRawFd;

    let config = nix::libc::sock_txtime {
        clockid: libc::CLOCK_MONOTONIC,
        flags: 0,
    };

    // mio::net::UdpSocket doesn't implement AsFd (yet?).
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(sock.as_raw_fd()) };

    setsockopt(&fd, TxTime, &config)?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_txtime_sockopt(_: &mio::net::UdpSocket) -> std::io::Result<()> {
    use std::io::Error;
    use std::io::ErrorKind;

    Err(Error::new(
        ErrorKind::Other,
        "Not supported on this platform",
    ))
}

const NANOS_PER_SEC: u64 = 1_000_000_000;

const INSTANT_ZERO: std::time::Instant = unsafe { std::mem::transmute(std::time::UNIX_EPOCH) };

pub fn std_time_to_u64(time: &std::time::Instant) -> u64 {
    let raw_time = time.duration_since(INSTANT_ZERO);
    let sec = raw_time.as_secs();
    let nsec = raw_time.subsec_nanos();
    sec * NANOS_PER_SEC + nsec as u64
}

#[cfg(target_os = "linux")]
pub fn send_with_pacing(
    socket: &mio::net::UdpSocket,
    buf: &[u8],
    send_info: &quiche::SendInfo,
    enable_gso: bool,
    segment_size: u16,
) -> std::io::Result<usize> {
    use nix::sys::socket::sendmsg;
    use nix::sys::socket::ControlMessage;
    use nix::sys::socket::MsgFlags;
    use nix::sys::socket::SockaddrStorage;
    use std::io::IoSlice;
    use std::os::unix::io::AsRawFd;

    let iov = [IoSlice::new(buf)];
    let dst = SockaddrStorage::from(send_info.to);
    let sockfd = socket.as_raw_fd();

    // Pacing option.
    let send_time = std_time_to_u64(&send_info.at);
    let cmsg_txtime = ControlMessage::TxTime(&send_time);

    let mut cmgs = vec![cmsg_txtime];
    if enable_gso {
        cmgs.push(ControlMessage::UdpGsoSegments(&segment_size));
    }

    match sendmsg(sockfd, &iov, &cmgs, MsgFlags::empty(), Some(&dst)) {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn send_with_pacing(
    socket: &mio::net::UdpSocket,
    buf: &[u8],
    send_info: &quiche::SendInfo,
    _enable_gso: bool,
    _segment_size: u16,
) -> std::io::Result<usize> {
    use nix::sys::socket::sendmsg;
    use nix::sys::socket::MsgFlags;
    use nix::sys::socket::SockaddrStorage;
    use std::io::IoSlice;
    use std::os::unix::io::AsRawFd;

    let iov = [IoSlice::new(buf)];
    let dst = SockaddrStorage::from(send_info.to);
    let sockfd = socket.as_raw_fd();
    match sendmsg(sockfd, &iov, &[], MsgFlags::empty(), Some(&dst)) {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}
