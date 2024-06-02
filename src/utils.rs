use anyhow::Result;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::{io, time};
use tokio::net::TcpStream;

use crate::structs::{StreamEntryResult, StreamType};

pub fn get_random_string(len: usize) -> String {
    let mut random_string = String::new();
    for _ in 0..len {
        let random_value = RandomState::new().build_hasher().finish() as usize;
        random_string.push_str(&random_value.to_string());
    }
    random_string
}

pub fn decode_hex_string(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(anyhow::anyhow!("Invalid hex string length"));
    }

    let mut binary_data = Vec::new();

    for i in (0..hex.len()).step_by(2) {
        let byte_str = &hex[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)?;
        binary_data.push(byte);
    }

    Ok(binary_data)
}

pub fn clear_read_buffer_from_tcp_stream(stream: &mut TcpStream) {
    let mut buf = [0; 512];
    loop {
        match stream.try_read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(_) => {
                continue;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Would block");
                break;
            }
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                break;
            }
        }
    }
}

pub fn validate_and_generate_entry_id(
    stream: &StreamType,
    entry_id: String,
) -> Result<StreamEntryResult> {
    let latest_entry_id = if let Some(stream_last) = stream.last() {
        stream_last.0.clone()
    } else {
        "0-0".to_string()
    };

    let latest_entry_id_parts: Vec<&str> = latest_entry_id.split('-').collect();

    let new_entry_id_parts = if entry_id == "*" {
        vec!["*", "*"]
    } else {
        entry_id.split('-').collect::<Vec<&str>>()
    };

    let latest_entry_id_timestamp = latest_entry_id_parts[0].parse::<u64>()?;
    let latest_entry_id_sequence = latest_entry_id_parts[1].parse::<u64>()?;
    let new_entry_id_timestamp = if new_entry_id_parts[0] == "*" {
        // use the current unix time in milliseconds as the timestamp
        time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)?
            .as_millis() as u64
    } else {
        new_entry_id_parts[0].parse::<u64>()?
    };

    let latest_entry_id_sequence_matching_timestamp: Option<u64> =
        if new_entry_id_timestamp <= latest_entry_id_timestamp {
            Some(latest_entry_id_sequence)
        } else {
            None
        };

    let new_entry_id_sequence = if new_entry_id_parts[1] == "*" {
        if let Some(entry_id) = latest_entry_id_sequence_matching_timestamp {
            entry_id + 1
        } else {
            match new_entry_id_timestamp {
                0 => 1,
                _ => 0,
            }
        }
    } else {
        new_entry_id_parts[1].parse::<u64>()?
    };
    println!("new entry id sequence: {}", new_entry_id_sequence);

    let error_msg = if new_entry_id_timestamp <= 0 && new_entry_id_sequence <= 0 {
        "-ERR The ID specified in XADD must be greater than 0-0\r\n"
    } else {
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    };
    println!(
        "new entry id timestamp: {}, latest entry id timestamp: {}",
        new_entry_id_timestamp, latest_entry_id_timestamp
    );
    if new_entry_id_timestamp < latest_entry_id_timestamp {
        return Ok(StreamEntryResult::ErrorMessage(error_msg.to_string()));
    } else if new_entry_id_timestamp == latest_entry_id_timestamp {
        let latest_entry_id_sequence = latest_entry_id_parts[1].parse::<u64>()?;
        if new_entry_id_sequence <= latest_entry_id_sequence {
            return Ok(StreamEntryResult::ErrorMessage(error_msg.to_string()));
        }
    }
    let new_entry_id = format!("{}-{}", new_entry_id_timestamp, new_entry_id_sequence);
    println!("new entry id: {}", new_entry_id);
    Ok(StreamEntryResult::EntryId(new_entry_id))
}
