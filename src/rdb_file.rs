//use anyhow::anyhow;
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

pub struct RDBFileResult {
    pub key_value_mapping: HashMap<String, String>,
}

pub fn parse_rdb_file_at_path(path: &str) -> Result<RDBFileResult> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let bytes_read = read_magic_redis_string(&buffer)?;
    buffer.drain(0..bytes_read as usize);
    let (redis_version_bytes_read, _version) = read_redis_version(&buffer)?;
    buffer.drain(0..redis_version_bytes_read as usize);
    let mut result = RDBFileResult {
        key_value_mapping: HashMap::new(),
    };
    let mut cur_key: Option<String> = None;
    loop {
        if buffer.is_empty() {
            break;
        }
        match buffer[0] {
            // aux field
            0xFA => {
                let (aux_field_bytes_read, aux_field, aux_val) = read_aux_field(&buffer)?;
                buffer.drain(0..aux_field_bytes_read as usize);
                println!("Aux field: {}, val {}", aux_field, aux_val);
            }
            // eof marker
            0xFF => {
                // end of file - there's an 8 byte checksum at the end after this marker
                //if buffer.len() != 9 {
                //    println!("buffer len: {}", buffer.len());
                //    return Err(anyhow::anyhow!("Invalid RDB file"));
                //}
                println!("End of file");
                break;
            }
            // selectdb
            0xFE => {
                // length encoded field
                let (bytes_read, db_num) = handle_reading_rdb_string(&buffer[1..])?;
                println!("bytes_read: {}", bytes_read);
                println!("db_num: {}", db_num);
                buffer.drain(0..bytes_read as usize + 1);
                println!("Select db: {}", db_num);
            }
            // resizedb
            0xFB => {
                // 2 length encoded ints follow
                // first is the db hash table size, second is the expiry hash table size

                // db hash table size
                let (bytes_read, db_hash_table_size) = handle_reading_rdb_string(&buffer[1..])?;
                buffer.drain(0..bytes_read as usize + 1);
                println!("db_hash_table_size: {}", db_hash_table_size);
                // expiry hash table size
                let (bytes_read, expiry_hash_table_size) = handle_reading_rdb_string(&buffer)?;
                buffer.drain(0..bytes_read as usize);
                println!("expiry_hash_table_size: {}", expiry_hash_table_size);
            }
            _ => {
                println!("WE IN HERE");
                // we should be reading the actual data, finally
                let (bytes_read, string_val) = handle_reading_rdb_string(&buffer)?;
                println!("bytes_read: {}", bytes_read);
                println!("string_val: {}", string_val);
                buffer.drain(0..bytes_read as usize);

                if cur_key.is_none() {
                    cur_key = Some(string_val.clone());
                    result.key_value_mapping.insert(string_val, "".to_string());
                } else {
                    let key = result.key_value_mapping.keys().last().unwrap();
                    result.key_value_mapping.insert(key.to_string(), string_val);
                }
            }
        }
    }
    Ok(result)
}

fn read_magic_redis_string(buf: &[u8]) -> Result<u64> {
    let magic_string = "REDIS";
    let magic_string_len = magic_string.len();
    if buf.len() < magic_string_len {
        return Err(anyhow::anyhow!("Invalid RDB file"));
    }

    let magic = &buf[0..magic_string_len];
    if magic != magic_string.as_bytes() {
        return Err(anyhow::anyhow!("Invalid RDB file"));
    }

    Ok(5)
}

fn read_redis_version(buf: &[u8]) -> Result<(u64, String)> {
    // the version is a 4-byte string
    if buf.len() < 4 {
        return Err(anyhow::anyhow!("Invalid RDB file"));
    }
    let version =
        std::str::from_utf8(&buf[0..4]).map_err(|_| anyhow::anyhow!("Invalid version string"))?;
    println!("RDB version: {}", version);
    Ok((4, version.to_string()))
}

fn read_aux_field(buf: &[u8]) -> Result<(u64, String, String)> {
    // first byte is OxFA, then just a regular ASCII string til the next OxFA or key type byte (0
    // to 14)
    if buf.is_empty() && buf[0] != 0xFA {
        return Err(anyhow::anyhow!("Invalid RDB file"));
    }
    let (length_of_key, aux_field) = handle_reading_rdb_string(&buf[1..])?;
    println!("aux field {}", aux_field);

    // Add 1 for the OxFA
    let remaining_buf = &buf[length_of_key as usize + 1..];
    let (length_of_val, aux_val) = handle_reading_rdb_string(remaining_buf)?;

    // Add 1 for the OxFA
    let bytes_read = length_of_key + length_of_val + 1;
    Ok((bytes_read, aux_field.to_string(), aux_val.to_string()))
}

fn handle_reading_rdb_string(buf: &[u8]) -> Result<(u64, String)> {
    // the first 2 most significant bits of the first byte are the encoding type
    // https://rdb.fnordig.de/file_format.html#string-encoding
    // 00 - rest of the byte is the length of the string
    // 01 - next 14 bits are the length of the string
    // 10 - next 4 bytes are the length of the string - ignore the rest of the first byte
    // 11 - if next 6 bits are 0, then a 8 bit integer follows, if the next 6 bits are 1, then a 16 bit integer follows, if the next 6 bits are 2, then a 32 bit integer follows
    // if the next 6 bits are 3, then its a compressed LZF string
    // ints are stored in little endian format
    let most_significant_2_bits = buf[0] >> 6;
    let (length_of_string, string) = match most_significant_2_bits {
        0 => {
            println!("buf[0]: {}", buf[0]);
            let length_of_string = buf[0] & 0x3F;
            println!("length of string: {}", length_of_string);
            let string = std::str::from_utf8(&buf[1..length_of_string as usize + 1])
                .map_err(|_| anyhow::anyhow!("Invalid string"))?;
            // Add 1 for the first length byte
            (length_of_string as u64 + 1, string.to_string())
        }
        1 => {
            let length_of_string = (((buf[0] & 0x3F) as u16) << 8) | buf[1] as u16;
            let string = std::str::from_utf8(&buf[2..length_of_string as usize + 2])
                .map_err(|_| anyhow::anyhow!("Invalid string"))?;
            // Add 2 for the first 2 bytes needed to calculate the length
            (length_of_string as u64 + 2, string.to_string())
        }
        2 => {
            let length_of_string = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
            let string = std::str::from_utf8(&buf[5..length_of_string as usize + 5])
                .map_err(|_| anyhow::anyhow!("Invalid string"))?;
            // Add 4 for the first 4 bytes needed to calculate the length
            (length_of_string as u64 + 4, string.to_string())
        }
        3 => {
            let next_5_bits = buf[0] & 0x3F;
            match next_5_bits {
                0 => {
                    let int = buf[1];
                    // First byte indicated that an 8 bit integer follows
                    (2, int.to_string())
                }
                1 => {
                    let int = u16::from_le_bytes([buf[1], buf[2]]);
                    // First byte indicated that a 16 bit integer follows
                    (3, int.to_string())
                }
                2 => {
                    // First byte indicated that a 32 bit integer follows
                    let int = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    (5, int.to_string())
                }
                3 => {
                    // compressed LZF string
                    unimplemented!();
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid string"));
                }
            }
        }
        _ => {
            return Err(anyhow::anyhow!("Invalid string"));
        }
    };
    Ok((length_of_string, string))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rdb_file_at_path() {
        let path = "dump.rdb";
        parse_rdb_file_at_path(path).unwrap();
        //assert!(parse_rdb_file_at_path(path).is_ok());
    }
}
