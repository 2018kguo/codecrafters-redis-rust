use anyhow::Result;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

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
