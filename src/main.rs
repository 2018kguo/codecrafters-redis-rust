use std::io;

// Uncomment this block to pass the first stage
use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod serializer;

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // Primer on sockets: https://docs.python.org/3/howto/sockets.html
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                // 0 bytes read so connection is closed
                return Ok(());
            }
            Ok(_n) => {
                let (resp, _) = parse_resp_data(&buf)?;
                let commands_list = resp.serialize_to_list_of_lowercase_strings();
                match commands_list[0].as_str() {
                    "echo" => {
                        let echo_resp = RespData::unpack_array(&resp);
                        let serialized_resp = echo_resp[1].serialize_to_redis_protocol();
                        stream.write_all(serialized_resp.as_bytes()).await?;
                    }
                    "ping" => {
                        stream.write_all("+PONG\r\n".as_bytes()).await?;
                    }
                    _ => {
                        stream
                            .write_all("-ERR unknown command\r\n".as_bytes())
                            .await?;
                    }
                }
                // respond with PONG for this part
                // stream.write_all("+PONG\r\n".as_bytes()).await.unwrap();
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the fist stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                eprintln!("Error handling connection: {}", e);
            }
        });
    }
}
