use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::env;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::{io, time};
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
    master_replid: String,
    master_repl_offset: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut port_to_use = 6379;
    let mut replica_info: Option<String> = None;

    let port_flag_position = args.iter().position(|s| s == "--port");
    if let Some(pos) = port_flag_position {
        port_to_use = args[pos + 1].parse().unwrap();
    }
    let replica_flag_position = args.iter().position(|s| s == "--replicaof");
    if let Some(pos) = replica_flag_position {
        replica_info = Some(args[pos + 1].clone());
    }

    // generate 40 character long random string for the master_replid
    let master_replid: String = get_random_string(40);
    let master_repl_offset = 0;

    let sock = format!("127.0.0.1:{}", port_to_use);
    let storage: Arc<Mutex<HashMap<String, StoredValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let server_info = Arc::new(Mutex::new(ServerInfo {
        role: match replica_info {
            Some(_) => "slave".to_string(),
            None => "master".to_string(),
        },
        master_replid,
        master_repl_offset,
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
                        // ex: redis-cli INFO replication
                        // still need to actually parse the argument but this works for now
                        let server_info = server_info.lock().await;
                        let role = &server_info.role;

                        let replication_info = format!(
                            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                            role, server_info.master_replid, server_info.master_repl_offset
                        );
                        let info_resp_response = RespData::BulkString(replication_info);
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

fn get_random_string(len: usize) -> String {
    let mut random_string = String::new();
    for _ in 0..len {
        let random_value = RandomState::new().build_hasher().finish() as usize;
        random_string.push_str(&random_value.to_string());
    }
    random_string
}
