// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the fist stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                // Primer on sockets: https://docs.python.org/3/howto/sockets.html
                let mut buf = [0; 512];
                loop {
                    match _stream.read(&mut buf) {
                        Ok(0) => {
                            // 0 bytes read so connection is closed
                            break;
                        }
                        Ok(n) => {
                            // respond with PONG for this part
                            _stream.write_all("+PONG\r\n".as_bytes()).unwrap();
                        }
                        Err(e) => {
                            eprintln!("Error reading stream: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
