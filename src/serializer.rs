use anyhow::{Context, Result};

use crate::{structs::StreamType, utils::compare_stream_entry_ids};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RespData {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespData>),
    Integer(isize),
}

impl RespData {
    pub fn serialize_to_redis_protocol(&self) -> String {
        match self {
            RespData::SimpleString(data) => format!("+{}\r\n", data),
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
            RespData::Integer(data) => format!(":{}\r\n", data),
        }
    }

    pub fn serialize_to_list_of_strings(&self, lowercase: bool) -> Vec<String> {
        let string_vec = match self {
            RespData::SimpleString(data) => vec![data.to_string()],
            RespData::BulkString(data) => vec![data.to_string()],
            RespData::Array(data) => {
                let mut serialized_data = Vec::new();
                for d in data {
                    serialized_data.append(&mut d.serialize_to_list_of_strings(lowercase));
                }
                serialized_data
            }
            RespData::Integer(data) => vec![data.to_string()],
        };
        if lowercase {
            string_vec.iter().map(|s| s.to_lowercase()).collect()
        } else {
            string_vec
        }
    }

    pub fn unpack_array(&self) -> Vec<RespData> {
        match self {
            RespData::Array(data) => data.clone(),
            _ => panic!("Expected an array"),
        }
    }
}

pub fn parse_resp_data(bytes: &[u8]) -> Result<(RespData, usize)> {
    match bytes[0] as char {
        '$' => parse_bulk_string(bytes),
        '*' => parse_array(bytes),
        '+' => parse_simple_string(bytes),
        ':' => parse_integer(bytes),
        _ => {
            println!("Failed to parse {:?}", bytes);
            Err(anyhow::anyhow!("Failed to parse"))
        }
    }
}

fn parse_integer(bytes: &[u8]) -> Result<(RespData, usize)> {
    let (data, bytes_read) = read_until_crlf(&bytes[1..]).context("Failed to read until CRLF")?;
    let data = std::str::from_utf8(data).context("Failed to parse data")?;
    Ok((
        RespData::Integer(data.parse::<isize>().context("Failed to parse integer")?),
        bytes_read + 1,
    ))
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

    // check if this is a bulk string for an RDB file, in which base the starting 5 bytes after the length will be the string "REDIS"
    // and FF (255 in decimal) will be towards the end of the string.
    // if it is, we return a dummy BulkString with the string "REDIS" and the length of the string
    if len > 5
        && std::str::from_utf8(&bytes[start_of_bulk_string_index..start_of_bulk_string_index + 5])
            .unwrap()
            .to_uppercase()
            == "REDIS"
        && bytes[start_of_bulk_string_index..end_of_bulk_string_index].contains(&255)
    {
        return Ok((
            RespData::BulkString("__REDIS_RDB_FILE".to_string()),
            end_of_bulk_string_index,
        ));
    }
    let data = std::str::from_utf8(&bytes[start_of_bulk_string_index..end_of_bulk_string_index])
        .context("Failed to parse data")?;
    // add 2 to account for the \r\n at the end of the string
    Ok((
        RespData::BulkString(data.to_string()),
        end_of_bulk_string_index + 2,
    ))
}

fn parse_simple_string(_bytes: &[u8]) -> Result<(RespData, usize)> {
    let (data, bytes_read) = read_until_crlf(&_bytes[1..]).context("Failed to read until CRLF")?;
    Ok((
        RespData::SimpleString(
            std::str::from_utf8(data)
                .context("Failed to parse data")?
                .to_string(),
        ),
        bytes_read + 1,
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
    None
}

pub fn filter_and_serialize_stream_to_resp_data(
    stream: &StreamType,
    min_entry_id: Option<&str>,
    max_entry_id: Option<&str>,
) -> RespData
// returns a resp array of arrays,
    // [
    //  [
    //    "1526985054069-0",
    //    [
    //      "temperature",
    //      "36",
    //      "humidity",
    //      "95"
    //    ]
    //  ],
    //  [
    //    "1526985054079-0",
    //    [
    //      "temperature",
    //      "37",
    //      "humidity",
    //      "94"
    //    ]
    //  ],
{
    let mut resp_array = Vec::new();
    for entry in stream {
        let mut entry_array = Vec::new();
        entry_array.push(RespData::BulkString(entry.0.clone()));
        let mut entry_data = Vec::new();
        let entry_key = &entry.0.as_str();

        let is_ge_than_min = min_entry_id.is_none()
            || min_entry_id.unwrap() == "-"
            || (compare_stream_entry_ids(min_entry_id.unwrap(), entry_key) <= 0);
        let is_le_than_max = max_entry_id.is_none()
            || max_entry_id.unwrap() == "+"
            || (compare_stream_entry_ids(entry_key, max_entry_id.unwrap()) <= 0);

        if !(is_ge_than_min && is_le_than_max) {
            continue;
        }

        for (key, value) in entry.1.as_slice() {
            entry_data.push(RespData::BulkString(key.to_string()));
            entry_data.push(RespData::BulkString(value.to_string()));
        }
        entry_array.push(RespData::Array(entry_data));
        resp_array.push(RespData::Array(entry_array));
    }
    RespData::Array(resp_array)
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
    fn test_parse_simple_string() {
        let bytes = b"+OK\r\n";
        let (resp_data, bytes_read) = parse_simple_string(bytes).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(resp_data, RespData::SimpleString("OK".to_string()));
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
            resp_data.serialize_to_list_of_strings(true),
            vec!["echo", "hey"]
        );
    }

    #[test]
    fn test_serialize_multiline_bulk_string() {
        let bytes = b"$11\r\nhello\nworld\r\n";
        let (resp_data, bytes_read) = parse_bulk_string(bytes).unwrap();
        assert_eq!(bytes_read, 18);
        assert_eq!(resp_data, RespData::BulkString("hello\nworld".to_string()));
    }

    #[test]
    fn test_handle_bulk_string_after_simple_string() {
        let bytes = [
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 55, 53, 99, 100, 55, 98, 99, 49, 48,
            99, 52, 57, 48, 52, 55, 101, 48, 100, 49, 54, 51, 54, 54, 48, 102, 51, 98, 57, 48, 54,
            50, 53, 98, 49, 97, 102, 51, 49, 100, 99, 32, 48, 13, 10, 36, 56, 56, 13, 10, 82, 69,
            68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55,
            46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250,
            5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109,
            101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0,
            255, 240, 110, 59, 254, 192, 255, 90, 162, 0, 0, 0,
        ];
        let (resp_data, bytes_read) = parse_resp_data(&bytes).unwrap();
        assert!(
            resp_data
                == RespData::SimpleString(
                    "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0".to_string()
                )
        );
        let next_slice = &bytes[bytes_read..];
        let index_of_36 = bytes.iter().position(|&x| x == 36).unwrap();
        assert!(bytes_read == index_of_36);
        assert!(next_slice[0] == 36);
    }

    #[test]
    fn test_edge_case() {
        let bytes = [
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 55, 53, 99, 100, 55, 98, 99, 49, 48,
            99, 52, 57, 48, 52, 55, 101, 48, 100, 49, 54, 51, 54, 54, 48, 102, 51, 98, 57, 48, 54,
            50, 53, 98, 49, 97, 102, 51, 49, 100, 99, 32, 48, 13, 10, 36, 56, 56, 13, 10, 82, 69,
            68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55,
            46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250,
            5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109,
            101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0,
            255, 240, 110, 59, 254, 192, 255, 90, 162, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let (resp_data, bytes_read) = parse_resp_data(&bytes).unwrap();
        assert_eq!(
            resp_data,
            RespData::SimpleString(
                "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0".to_string()
            )
        );
        let (resp_data, bytes_read_2) = parse_resp_data(&bytes[bytes_read..]).unwrap();
        assert_eq!(
            resp_data,
            RespData::BulkString("__REDIS_RDB_FILE".to_string())
        );
        // assert that the leftover bytes are all null bytes
        assert!(bytes[bytes_read + bytes_read_2..].iter().all(|&x| x == 0));
    }

    #[test]
    fn test_parse_integer() {
        let bytes = b":1000\r\n";
        let (resp_data, bytes_read) = parse_integer(bytes).unwrap();
        assert_eq!(bytes_read, 7);
        assert_eq!(resp_data, RespData::Integer(1000));
    }

    #[test]
    fn test_parse_negative_integer() {
        let bytes = b":-1000\r\n";
        let (resp_data, bytes_read) = parse_integer(bytes).unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(resp_data, RespData::Integer(-1000));
    }
}
