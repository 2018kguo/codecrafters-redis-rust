use std::collections::HashMap;
use std::{io, time};

// Uncomment this block to pass the first stage
use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use std::env;
use std::sync::Arc;
use time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
mod serializer;

struct StoredValue {
    value: String,
    expiry: Option<Instant>,
}

struct ServerInfo {
    role: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut port_to_use = 6379;
    let port_flag_position = args.iter().position(|s| s == "--port");
    if let Some(pos) = port_flag_position {
        port_to_use = args[pos + 1].parse().unwrap();
    }

    let sock = format!("127.0.0.1:{}", port_to_use);
    let storage: Arc<Mutex<HashMap<String, StoredValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let server_info = Arc::new(Mutex::new(ServerInfo {
        role: "master".to_string(),
    }));
    let listener = TcpListener::bind(sock).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let storage = storage.clone();
        let server_info = server_info.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, storage, server_info).await {
                eprintln!("Error handling connection: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
) -> Result<()> {
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
                let commands_list = resp.serialize_to_list_of_strings(true);
                match commands_list[0].as_str() {
                    "echo" => {
                        let echo_resp = RespData::unpack_array(&resp);
                        let serialized_resp = echo_resp[1].serialize_to_redis_protocol();
                        stream.write_all(serialized_resp.as_bytes()).await?;
                    }
                    "ping" => {
                        stream.write_all("+PONG\r\n".as_bytes()).await?;
                    }
                    "set" => {
                        let set_resp = RespData::unpack_array(&resp);
                        // the second element in the array is the key
                        let key_str = set_resp[1].serialize_to_list_of_strings(false)[0].clone();
                        // the third element in the array is the value
                        let value_str = set_resp[2].serialize_to_list_of_strings(false)[0].clone();
                        // px argument is being provided
                        if set_resp.len() > 3 {
                            let arg = set_resp[3].serialize_to_list_of_strings(true)[0].clone();
                            if arg == "px" {
                                let expiry_milliseconds: usize = set_resp[4]
                                    .serialize_to_list_of_strings(false)[0]
                                    .clone()
                                    .parse()?;
                                let expiry = Instant::now()
                                    + Duration::from_millis(expiry_milliseconds as u64);
                                let mut held_storage = storage.lock().await;
                                held_storage.insert(
                                    key_str,
                                    StoredValue {
                                        value: value_str,
                                        expiry: Some(expiry),
                                    },
                                );
                            } else {
                                return Err(anyhow::anyhow!("Invalid argument"));
                            }
                        } else {
                            let mut held_storage = storage.lock().await;
                            held_storage.insert(
                                key_str,
                                StoredValue {
                                    value: value_str,
                                    expiry: None,
                                },
                            );
                        }
                        // return OK as a simple string
                        stream.write_all("+OK\r\n".as_bytes()).await?;
                    }
                    "get" => {
                        println!("Received get command");
                        let get_resp = RespData::unpack_array(&resp);
                        let key_str = get_resp[1].serialize_to_list_of_strings(false)[0].clone();
                        let held_storage = storage.lock().await;
                        match held_storage.get(&key_str) {
                            Some(stored_value)
                                if stored_value.expiry.is_none()
                                    || stored_value.expiry.unwrap() > Instant::now() =>
                            {
                                let string_value = stored_value.value.clone();
                                let resp_response = RespData::SimpleString(string_value);
                                stream
                                    .write_all(
                                        resp_response.serialize_to_redis_protocol().as_bytes(),
                                    )
                                    .await?;
                            }
                            // Key has either expired or never existed in the map.
                            _ => {
                                println!("Key not found");
                                stream.write_all("$-1\r\n".as_bytes()).await?;
                            }
                        }
                    }
                    "info" => {
                        let server_info = server_info.lock().await;
                        let role = &server_info.role;

                        let info_resp_response = RespData::BulkString(format!("role:{}", role));
                        stream
                            .write_all(info_resp_response.serialize_to_redis_protocol().as_bytes())
                            .await?;
                    }
                    _ => {
                        stream
                            .write_all("-ERR unknown command\r\n".as_bytes())
                            .await?;
                    }
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}
