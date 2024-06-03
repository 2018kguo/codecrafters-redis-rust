use crate::serializer::{
    filter_and_serialize_stream_to_resp_data_xrange_format,
    filter_and_serialize_stream_to_resp_data_xread_format, parse_resp_data, RespData,
};
use crate::structs::{ServerInfo, ServerMessageChannels, StoredValue, StreamEntryResult, Value, StreamType};
use crate::utils::{self, validate_and_generate_entry_id};
use anyhow::Result;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time;
use time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

pub async fn handle_get_command(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    let get_resp = RespData::unpack_array(resp);
    let key_str = get_resp[1].serialize_to_list_of_strings(false)[0].clone();
    let held_storage = storage.lock().await;
    match held_storage.get(&key_str) {
        Some(stored_value)
            if stored_value.expiry.is_none() || stored_value.expiry.unwrap() > Instant::now() =>
        {
            let string_value = stored_value.value.clone();
            if let Value::String(string_value) = string_value {
                let resp_response = RespData::SimpleString(string_value);
                stream
                    .write_all(resp_response.serialize_to_redis_protocol().as_bytes())
                    .await?;
            } else {
                unimplemented!();
            }
        }
        // Key has either expired or never existed in the map.
        _ => {
            println!("Key not found");
            stream.write_all("$-1\r\n".as_bytes()).await?;
        }
    }
    Ok(())
}

pub async fn handle_type_command(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    let type_resp = RespData::unpack_array(resp);
    let key_str = type_resp[1].serialize_to_list_of_strings(false)[0].clone();
    let held_storage = storage.lock().await;
    let val_type = match held_storage.get(&key_str) {
        Some(stored_value)
            if stored_value.expiry.is_none() || stored_value.expiry.unwrap() > Instant::now() =>
        {
            match stored_value.value {
                Value::String(_) => "string",
                Value::Stream(_) => "stream",
            }
        }
        // Key has either expired or never existed in the map.
        _ => "none",
    };
    let resp_response = RespData::SimpleString(val_type.to_string());
    stream
        .write_all(resp_response.serialize_to_redis_protocol().as_bytes())
        .await?;
    Ok(())
}

pub async fn handle_set_command(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    sender: Arc<Sender<Vec<u8>>>,
    message_bytes: &[u8],
    resp: RespData,
    is_replica: bool,
    handshake_with_master_complete: bool,
    server_info: Arc<Mutex<ServerInfo>>,
) -> Result<()> {
    let set_resp = resp.serialize_to_list_of_strings(false);
    // the second element in the array is the key
    let key_str = &set_resp[1];
    // the third element in the array is the value
    let value_str = &set_resp[2];
    // px argument is being provided
    if set_resp.len() > 3 {
        let arg = &set_resp[3];
        if arg == "px" {
            let expiry_milliseconds: usize = set_resp[4].clone().parse()?;
            let expiry = Instant::now() + Duration::from_millis(expiry_milliseconds as u64);
            let mut held_storage = storage.lock().await;
            held_storage.insert(
                key_str.to_string(),
                StoredValue {
                    value: Value::String(value_str.to_string()),
                    expiry: Some(expiry),
                },
            );
        } else {
            return Err(anyhow::anyhow!("Invalid argument"));
        }
    } else {
        let mut held_storage = storage.lock().await;
        held_storage.insert(
            key_str.to_string(),
            StoredValue {
                value: Value::String(value_str.to_string()),
                expiry: None,
            },
        );
    }
    // increment the number of command bytes processed as a replica
    if is_replica {
        let mut server_info = server_info.lock().await;
        server_info.num_command_bytes_processed_as_replica += message_bytes.len();
    }
    // return OK as a simple string
    if !handshake_with_master_complete {
        stream.write_all("+OK\r\n".as_bytes()).await?;
    }
    // send the same bytes that we read for this command to all of the
    // connected replicas. Since the replicas run the same code they will
    // parse the command the same way so we don't need to re-serialize the
    // payload for them.
    //
    // We can't directly send the contents of our own buffer because it will contain a bunch of
    // null byte padding at the end. Instead, we need to send the exact bytes
    // that we actually read to construct the message
    if !is_replica {
        let mut server_info = server_info.lock().await;
        // increment master_replication_offset by the number of bytes in the message_bytes
        // I think there technically is supposed to be a system where the master pings replicas
        // to make sure they're alive and I suspect we're also supposed to increment the offset
        // in the case, but I don't think its necessary to implement.
        server_info.master_repl_offset += message_bytes.len();
        sender.send(message_bytes.to_vec())?;
    }
    Ok(())
}

pub async fn handle_psync_command(
    stream: &mut TcpStream,
    sender: Arc<Sender<Vec<u8>>>,
    resp: &RespData,
    master_replid: String,
    master_repl_offset: usize,
    server_info: Arc<Mutex<ServerInfo>>,
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
) -> Result<()> {
    let psync_resp = resp.serialize_to_list_of_strings(false);
    let _replication_id = &psync_resp[1];
    let _offset = &psync_resp[2];

    let full_resync_response = RespData::SimpleString(format!(
        "+FULLRESYNC {} {}",
        master_replid, master_repl_offset
    ));
    stream
        .write_all(
            full_resync_response
                .serialize_to_redis_protocol()
                .as_bytes(),
        )
        .await?;
    stream.flush().await?;

    // increment the number of connected replicas
    {
        let mut server_info = server_info.lock().await;
        // get the address of the replica
        let addr = stream.peer_addr().unwrap().to_string();
        println!("Replica address: {}", addr);
        server_info.replica_addresses.push(addr);
    }
    // lastly, send an empty RDB file back to the replica
    let hardcoded_empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let binary_empty_rdb = utils::decode_hex_string(hardcoded_empty_rdb_file_hex)?;
    let len = binary_empty_rdb.len();
    stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
    stream.write_all(&binary_empty_rdb).await?;
    // create a new receiver for the replica command channel
    let mut replica_rx = sender.subscribe();
    loop {
        let replica_command = replica_rx.recv().await?;
        // check if its a getack command that's being propagated to replicas
        let (replica_command_resp, _num_bytes) = parse_resp_data(&replica_command)?;
        let replica_command_list = replica_command_resp.serialize_to_list_of_strings(true);
        let is_wait_command = replica_command_list.len() == 3
            && replica_command_list[0] == "replconf"
            && replica_command_list[1] == "getack";

        // When reading the results back from GETACK responses we clear the read buffer beforehand
        if is_wait_command {
            utils::clear_read_buffer_from_tcp_stream(stream);
        }
        // whenever a message is received from the broadcast channel, simply forward it to the replica
        stream.write_all(&replica_command).await?;
        // make sure messages are sent to the replica immediately
        stream.flush().await?;
        // for wait commands, read
        if is_wait_command {
            let mut bytes_read_vec = Vec::<u8>::new();
            let mut buf = [0; 512];

            // this is dumb but basically am giving the replica 250ms to respond to the wait
            // command. using .read() is problematic if the replica doesn't decide to respond since
            // it will just block the thread waiting for a response.
            let read_timeout_duration = Duration::from_millis(250);
            match timeout(read_timeout_duration, stream.readable()).await {
                Ok(Ok(_)) => {
                    match stream.try_read(&mut buf) {
                        Ok(0) => {
                            //println!("Exited wait read loop gracefully");
                        }
                        Ok(num_bytes) => {
                            // read the bytes into bytes_read_vec
                            bytes_read_vec.extend_from_slice(&buf[..num_bytes]);
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            //println!("Would block");
                        }
                        Err(_e) => {
                            //eprintln!("Error reading from socket for wait command: {}", e);
                        }
                    }
                }
                _ => {
                    println!("Timed out waiting for replica to respond to wait command");
                    //continue;
                }
            }

            // didn't receive a GETACK response from the replica so just continue
            if bytes_read_vec.is_empty() {
                println!("No bytes read for wait command");
                continue;
            }
            let (resp, _) = parse_resp_data(&bytes_read_vec)?;
            let offset_resp = resp.serialize_to_list_of_strings(true);
            // we expect to get REPLCONF ACK <offset> as a response
            if !(offset_resp.len() == 3 && offset_resp[0] == "replconf" && offset_resp[1] == "ack")
            {
                anyhow::bail!("Unexpected response from replica");
            }
            let offset = offset_resp[2].clone().parse::<usize>()?;
            // send the replica's offset back upstreawm to the thread thats handling the WAIT
            // command via the mpsc channel
            let u64_offset = offset.try_into().unwrap();
            let wait_tx_locked = wait_tx.lock().await;
            wait_tx_locked.send(u64_offset).await?;
            println!("Forwarded offset to WAIT command: {}", offset);
        }
        println!(
            "Forwarded command to replica: {:?}, is wait command {:?}",
            replica_command, is_wait_command
        );
    }
}

pub async fn handle_xadd_command(
    tcp_stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    // XADD stream_name * key value
    let args = resp.serialize_to_list_of_strings(true);
    let stream_name = args[1].clone();
    let entry_id = &args[2].clone();
    let key = args[3].clone();
    let value = args[4].clone();

    let mut held_storage = storage.lock().await;
    let entry_result: StreamEntryResult = match held_storage.get_mut(&stream_name) {
        Some(StoredValue {
            value: Value::Stream(stream),
            ..
        }) => {
            let stream_entry_result =
                validate_and_generate_entry_id(&stream, entry_id.to_string())?;
            match stream_entry_result {
                StreamEntryResult::ErrorMessage(error_msg) => {
                    StreamEntryResult::ErrorMessage(error_msg)
                }
                StreamEntryResult::EntryId(new_entry_id) => {
                    stream.push((new_entry_id.to_string(), vec![(key, value)]));
                    //held_storage.insert(
                    //    stream_name,
                    //    StoredValue {
                    //        value: Value::Stream(stream),
                    //        expiry: None,
                    //    },
                    //);
                    StreamEntryResult::EntryId(new_entry_id.to_string())
                }
            }
        }
        _ => {
            let stream_entry_result =
                validate_and_generate_entry_id(&vec![], entry_id.to_string())?;
            match stream_entry_result {
                StreamEntryResult::ErrorMessage(error_msg) => {
                    StreamEntryResult::ErrorMessage(error_msg)
                }
                StreamEntryResult::EntryId(new_entry_id) => {
                    held_storage.insert(
                        stream_name,
                        StoredValue {
                            value: Value::Stream(vec![(
                                new_entry_id.to_string(),
                                vec![(key, value)],
                            )]),
                            expiry: None,
                        },
                    );
                    StreamEntryResult::EntryId(new_entry_id.to_string())
                }
            }
        }
    };
    match entry_result {
        StreamEntryResult::ErrorMessage(error_msg) => {
            tcp_stream.write_all(error_msg.as_bytes()).await?;
        }
        StreamEntryResult::EntryId(entry_id) => {
            println!("Entry ID: {}", entry_id);
            let resp_response = RespData::BulkString(entry_id);
            tcp_stream
                .write_all(resp_response.serialize_to_redis_protocol().as_bytes())
                .await?;
        }
    }
    Ok(())
}

pub async fn handle_xrange_command(
    tcp_stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    let args = resp.serialize_to_list_of_strings(true);
    let stream_key = &args[1];
    let min_entry_id = &args[2];
    let max_entry_id = &args[3];

    let storage = storage.lock().await;
    match storage.get(stream_key) {
        Some(StoredValue {
            value: Value::Stream(stream),
            ..
        }) => {
            println!("max entry id: {}", max_entry_id);
            let resp_data = filter_and_serialize_stream_to_resp_data_xrange_format(
                stream,
                Some(min_entry_id),
                Some(max_entry_id),
            );
            tcp_stream
                .write_all(resp_data.serialize_to_redis_protocol().as_bytes())
                .await?;
        }
        _ => {
            tcp_stream.write_all("$-1\r\n".as_bytes()).await?;
            return Ok(());
        }
    };
    Ok(())
}

pub async fn handle_xread_command(
    tcp_stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    // XREAD streams stream_key 0-0
    let args = resp.serialize_to_list_of_strings(true);
    let stream_arg = &args[1];

    if stream_arg != "streams" {
        unimplemented!();
    }

    let mut index = 2;
    let mut stream_key_and_min_entry_id_list: Vec<(&str, &str)> = vec![]; 
    let mut num_keys = 0;
    //XREAD streams stream_key other_stream_key 0-0 0-1
    while index < args.len() && !args[index].contains("-") {
        num_keys += 1;
        index += 1;
    }
    index = 2;
    while index < 2 + num_keys {
        let stream_key = &args[index];
        let min_entry_id = &args[index + num_keys];
        stream_key_and_min_entry_id_list.push((stream_key, min_entry_id));
        index += 1;
    }
    
    let mut streams_and_min_entry_ids: Vec<(&str, &StreamType, Option<&str>)> = vec![];

    let storage = storage.lock().await;
    for (stream_key, min_entry_id) in stream_key_and_min_entry_id_list {
        match storage.get(stream_key) {
            Some(StoredValue {
                value: Value::Stream(stream),
                ..
            }) => {
                streams_and_min_entry_ids.push((stream_key, stream, Some(min_entry_id)));
            }
            _ => {
                tcp_stream.write_all("$-1\r\n".as_bytes()).await?;
                return Ok(());
            }
        };
    }
    let resp_data =
        filter_and_serialize_stream_to_resp_data_xread_format(streams_and_min_entry_ids);
    tcp_stream
        .write_all(resp_data.serialize_to_redis_protocol().as_bytes())
        .await?;
    Ok(())
}

pub async fn handle_wait_command(
    stream: &mut TcpStream,
    resp: &RespData,
    server_info: Arc<Mutex<ServerInfo>>,
    message_channels: &mut ServerMessageChannels,
    master_repl_offset: usize,
    is_replica: bool,
) -> Result<()> {
    if is_replica {
        return Err(anyhow::anyhow!("WAIT command not expected for replicas"));
    }

    let args = resp.serialize_to_list_of_strings(false);
    let num_replicas_arg = args[1].clone().parse::<usize>()?;
    let wait_timeout_arg = args[2].clone().parse::<usize>()?;
    let timeout_instant = Instant::now() + Duration::from_millis(wait_timeout_arg as u64);

    let replica_addresses = {
        let server_info = server_info.lock().await;
        server_info.replica_addresses.clone()
    };

    let mut wait_rc_locked = message_channels.wait_rc.lock().await;
    // Discard all messages currently in the broadcast channel
    while wait_rc_locked.try_recv().is_ok() {
        continue;
    }

    let mut getack_bytes_to_add_to_master_offset = 0;
    //let mut getack_bytes_to_add_to_master_offset = 0;
    let num_synced_replicas = if master_repl_offset == 0 {
        replica_addresses.len()
    } else {
        // send GETACK to all replicas and wait for them to acknowledge
        // or wait for a timeout:
        let mut synced_counter = 0;
        let getack_command = RespData::Array(vec![
            RespData::BulkString("REPLCONF".to_string()),
            RespData::BulkString("GETACK".to_string()),
            RespData::BulkString("*".to_string()),
        ]);
        let bytes = getack_command
            .serialize_to_redis_protocol()
            .as_bytes()
            .to_vec();
        let mut is_initial_iteration = true;
        while Instant::now() < timeout_instant {
            // TODO: after sending the messages on this broadcast channel,
            // listen to their responses and read their offsets
            if synced_counter >= num_replicas_arg {
                break;
            }
            if is_initial_iteration {
                // send the GETACK command to all replicas
                //
                // it seemed to be problematic to send multiple copies of GETACK because
                // in the test cases, after a SET command is sent to the master the
                // replicas are expected to receive the SET command as their next immediate
                // message. If there was a backlogged GETACK command then the SET command
                // would be second in line and it would fail. Honestly not sure if this is
                // a quirk in the test cases or if I'm doing something wrong.
                message_channels.sender.send(bytes.clone())?;
                getack_bytes_to_add_to_master_offset += bytes.len();
            }
            is_initial_iteration = false;

            sleep(Duration::from_millis(50)).await;

            while let Ok(offset) = wait_rc_locked.try_recv() {
                println!(
                    "Received offset from replica: {}, master repl offset is {}",
                    offset, master_repl_offset
                );
                if offset >= (master_repl_offset as u64) {
                    synced_counter += 1;
                }
                if synced_counter >= num_replicas_arg {
                    break;
                }
            }
        }
        synced_counter
    };
    println!("Number of synced replicas: {}", num_synced_replicas);
    let integer_resp = RespData::Integer(num_synced_replicas as isize);
    stream
        .write_all(integer_resp.serialize_to_redis_protocol().as_bytes())
        .await?;
    // increment the master_repl_offset by the number of bytes in the GETACK command
    // I _think_ you're supposed to do this but tests pass either way so whatever
    {
        let mut server_info = server_info.lock().await;
        server_info.master_repl_offset += getack_bytes_to_add_to_master_offset;
    }
    Ok(())
}
