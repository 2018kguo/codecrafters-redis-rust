use anyhow::{Context, Result};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RespData {
    BulkString(String),
    Array(Vec<RespData>),
}

impl RespData {
    pub fn serialize_to_redis_protocol(&self) -> String {
        match self {
            RespData::BulkString(data) => format!("${}\r\n{}\r\n", data.len(), data),
            RespData::Array(data) => {
                let mut serialized_data = String::from("*");
                serialized_data.push_str(&data.len().to_string());
                serialized_data.push_str("\r\n");
                for d in data {
                    serialized_data.push_str(&d.serialize_to_redis_protocol());
                }
                serialized_data
            }
        }
    }

    pub fn serialize_to_list_of_lowercase_strings(&self) -> Vec<String> {
        match self {
            RespData::BulkString(data) => vec![data.to_lowercase()],
            RespData::Array(data) => {
                let mut serialized_data = Vec::new();
                for d in data {
                    serialized_data.append(&mut d.serialize_to_list_of_lowercase_strings());
                }
                serialized_data
            }
        }
    }

    pub fn unpack_array(&self) -> Vec<RespData> {
        match self {
            RespData::Array(data) => data.clone(),
            _ => panic!("Expected an array"),
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
pub fn parse_resp_data(bytes: &[u8]) -> Result<(RespData, usize)> {
    match bytes[0] as char {
        '$' => parse_bulk_string(bytes),
        '*' => parse_array(bytes),
        _ => unimplemented!(),
    }
}

fn parse_bulk_string(bytes: &[u8]) -> Result<(RespData, usize)> {
    let (len_str, bytes_read) =
        read_until_crlf(&bytes[1..]).context("Failed to read until CRLF")?;
    let len = std::str::from_utf8(len_str)
        .context("Failed to parse length")?
        .parse::<usize>()
        .context("Failed to parse length")?;
    // add 1 to account for the leading $ character
    let start_of_bulk_string_index = 1 + bytes_read;
    let end_of_bulk_string_index = 1 + bytes_read + len;
    let data = std::str::from_utf8(&bytes[start_of_bulk_string_index..end_of_bulk_string_index])
        .context("Failed to parse data")?;
    // add 2 to account for the \r\n at the end of the string
    Ok((
        RespData::BulkString(data.to_string()),
        end_of_bulk_string_index + 2,
    ))
}

fn parse_array(_bytes: &[u8]) -> Result<(RespData, usize)> {
    let (len_array, bytes_read) =
        read_until_crlf(&_bytes[1..]).context("Failed to read until CRLF")?;
    let len = std::str::from_utf8(len_array)
        .context("Failed to parse length")?
        .parse::<usize>()
        .context("Failed to parse length")?;
    // add 1 to account for the leading * character
    let mut index = 1 + bytes_read;
    let mut array_resp: Vec<RespData> = Vec::new();
    for _ in 0..len {
        let (data, bytes_read) = parse_resp_data(&_bytes[index..])?;
        index += bytes_read;
        array_resp.push(data);
    }
    Ok((RespData::Array(array_resp), index))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bulk_string() {
        let bytes = b"$3\r\nhey\r\n";
        let (resp_data, bytes_read) = parse_bulk_string(bytes).unwrap();
        assert_eq!(bytes_read, 9);
        assert_eq!(resp_data, RespData::BulkString("hey".to_string()));
    }

    #[test]
    fn test_parse_array() {
        let bytes = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let (resp_data, bytes_read) = parse_array(bytes).unwrap();
        assert_eq!(bytes_read, 23);
        assert_eq!(
            resp_data,
            RespData::Array(vec![
                RespData::BulkString("ECHO".to_string()),
                RespData::BulkString("hey".to_string())
            ])
        );
    }

    #[test]
    fn test_serialize_to_redis_protocol() {
        let resp_data = RespData::Array(vec![
            RespData::BulkString("ECHO".to_string()),
            RespData::BulkString("hey".to_string()),
        ]);
        assert_eq!(
            resp_data.serialize_to_redis_protocol(),
            "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        );
    }

    #[test]
    fn test_serialize_to_lowercase_strings() {
        let resp_data = RespData::Array(vec![
            RespData::BulkString("ECHO".to_string()),
            RespData::BulkString("hey".to_string()),
        ]);
        assert_eq!(
            resp_data.serialize_to_list_of_lowercase_strings(),
            vec!["echo", "hey"]
        );
    }
}
