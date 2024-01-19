use std::net::{SocketAddr, UdpSocket};
use std::time::Instant;

pub fn main() {

    // dallas, while true; do nc -lu 0.0.0.0 2323; done
    let server_addr: SocketAddr = "139.178.82.223:2323".parse().unwrap();
    let socket = UdpSocket::bind("0.0.0.0:0").expect("couldn't bind to address");

    {
        let buf = [b'x'; 100];
        println!("sending .. : {}", buf.len());
        let bytes_sent = socket.send_to(&buf, server_addr).expect("couldn't send data");
        println!("bytes_sent: {}", bytes_sent);
    }
    {
        let buf = [b'x'; 1420];
        println!("sending .. : {}", buf.len());
        let bytes_sent = socket.send_to(&buf, server_addr).expect("couldn't send data");
        println!("bytes_sent: {}", bytes_sent);
    }
    for i in (100..15000).step_by(500) {
        let buf = vec![b'x'; i];
        println!("sending .. : {}", buf.len());
        match socket.send_to(&buf, server_addr) {
            Ok(bytes_sent) => {
                println!("bytes_sent: {}", bytes_sent);
            }
            Err(error) => {
                println!("error: {}", error);
            }
        };
    }

}