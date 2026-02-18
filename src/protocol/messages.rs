// Wire protocol message types - TDD: tests written first, implementation follows

use bytes::Bytes;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Incomplete message")]
    Incomplete,
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DescribeKind {
    Statement,
    Portal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },
    SslRequest,
    Query(String),
    Parse {
        name: String,
        query: String,
        param_types: Vec<i32>,
    },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Describe {
        kind: DescribeKind,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    Sync,
    Terminate,
    Close {
        kind: DescribeKind,
        name: String,
    },
    Flush,
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationMD5Password { salt: [u8; 4] },
    ParameterStatus { name: String, value: String },
    BackendKeyData { process_id: i32, secret_key: i32 },
    ReadyForQuery { status: u8 },
    RowDescription { fields: Vec<FieldDescription> },
    DataRow { values: Vec<Option<Vec<u8>>> },
    CommandComplete { tag: String },
    ErrorResponse { severity: String, code: String, message: String },
    ParseComplete,
    BindComplete,
    NoData,
    EmptyQueryResponse,
    ParameterDescription { param_types: Vec<i32> },
    CopyInResponse { format: u8, column_formats: Vec<i16> },
}

// Helper to read a null-terminated string from Bytes
fn read_cstring(buf: &[u8], pos: &mut usize) -> Result<String, ProtocolError> {
    let start = *pos;
    while *pos < buf.len() {
        if buf[*pos] == 0 {
            let s = String::from_utf8(buf[start..*pos].to_vec())
                .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
            *pos += 1; // skip null
            return Ok(s);
        }
        *pos += 1;
    }
    Err(ProtocolError::Incomplete)
}

impl FrontendMessage {
    /// Parse the initial startup message (no message type byte)
    pub fn parse_startup(buf: &Bytes) -> Result<Self, ProtocolError> {
        if buf.len() < 8 {
            return Err(ProtocolError::Incomplete);
        }
        let length = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

        // SSL request
        if code == 80877103 {
            return Ok(FrontendMessage::SslRequest);
        }

        // Cancel request
        if code == 80877102 {
            return Err(ProtocolError::InvalidFormat("Cancel request not supported".into()));
        }

        // Startup message
        let version = code;
        let mut params = HashMap::new();
        let mut pos = 8;
        while pos < length {
            if buf[pos] == 0 {
                break;
            }
            let key = read_cstring(buf, &mut pos)?;
            let value = read_cstring(buf, &mut pos)?;
            params.insert(key, value);
        }

        Ok(FrontendMessage::Startup { version, params })
    }

    /// Parse a regular message (with type byte)
    pub fn parse(buf: &Bytes) -> Result<Self, ProtocolError> {
        if buf.len() < 5 {
            return Err(ProtocolError::Incomplete);
        }

        let msg_type = buf[0];
        let _length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        let body = &buf[5..];

        match msg_type {
            b'Q' => {
                // Query: string\0
                let mut pos = 0;
                let query = read_cstring(body, &mut pos)?;
                Ok(FrontendMessage::Query(query))
            }
            b'P' => {
                // Parse: name\0, query\0, Int16 nparams, [Int32 oid]*
                let mut pos = 0;
                let name = read_cstring(body, &mut pos)?;
                let query = read_cstring(body, &mut pos)?;
                let num_params = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2;
                let mut param_types = Vec::with_capacity(num_params);
                for _ in 0..num_params {
                    let oid = i32::from_be_bytes([
                        body[pos], body[pos + 1], body[pos + 2], body[pos + 3],
                    ]);
                    pos += 4;
                    param_types.push(oid);
                }
                Ok(FrontendMessage::Parse { name, query, param_types })
            }
            b'B' => {
                // Bind: portal\0, statement\0, Int16 nformats, [Int16]*,
                //        Int16 nparams, [Int32 len, bytes]*,
                //        Int16 nresult_formats, [Int16]*
                let mut pos = 0;
                let portal = read_cstring(body, &mut pos)?;
                let statement = read_cstring(body, &mut pos)?;

                let num_formats = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2;
                let mut param_formats = Vec::with_capacity(num_formats);
                for _ in 0..num_formats {
                    param_formats.push(i16::from_be_bytes([body[pos], body[pos + 1]]));
                    pos += 2;
                }

                let num_params = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2;
                let mut params = Vec::with_capacity(num_params);
                for _ in 0..num_params {
                    let len = i32::from_be_bytes([
                        body[pos], body[pos + 1], body[pos + 2], body[pos + 3],
                    ]);
                    pos += 4;
                    if len == -1 {
                        params.push(None);
                    } else {
                        let len = len as usize;
                        params.push(Some(body[pos..pos + len].to_vec()));
                        pos += len;
                    }
                }

                let num_result_formats = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2;
                let mut result_formats = Vec::with_capacity(num_result_formats);
                for _ in 0..num_result_formats {
                    result_formats.push(i16::from_be_bytes([body[pos], body[pos + 1]]));
                    pos += 2;
                }

                Ok(FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    params,
                    result_formats,
                })
            }
            b'D' => {
                // Describe: byte kind, string name\0
                let kind = match body[0] {
                    b'S' => DescribeKind::Statement,
                    b'P' => DescribeKind::Portal,
                    _ => return Err(ProtocolError::InvalidFormat("Invalid describe kind".into())),
                };
                let mut pos = 1;
                let name = read_cstring(body, &mut pos)?;
                Ok(FrontendMessage::Describe { kind, name })
            }
            b'E' => {
                // Execute: portal\0, Int32 max_rows
                let mut pos = 0;
                let portal = read_cstring(body, &mut pos)?;
                let max_rows = i32::from_be_bytes([
                    body[pos], body[pos + 1], body[pos + 2], body[pos + 3],
                ]);
                Ok(FrontendMessage::Execute { portal, max_rows })
            }
            b'S' => Ok(FrontendMessage::Sync),
            b'X' => Ok(FrontendMessage::Terminate),
            b'C' => {
                let kind = match body[0] {
                    b'S' => DescribeKind::Statement,
                    b'P' => DescribeKind::Portal,
                    _ => return Err(ProtocolError::InvalidFormat("Invalid close kind".into())),
                };
                let mut pos = 1;
                let name = read_cstring(body, &mut pos)?;
                Ok(FrontendMessage::Close { kind, name })
            }
            b'H' => Ok(FrontendMessage::Flush),
            b'd' => {
                // CopyData: raw bytes
                Ok(FrontendMessage::CopyData(body.to_vec()))
            }
            b'c' => Ok(FrontendMessage::CopyDone),
            b'f' => {
                // CopyFail: error message string\0
                let mut pos = 0;
                let msg = read_cstring(body, &mut pos)?;
                Ok(FrontendMessage::CopyFail(msg))
            }
            _ => Err(ProtocolError::InvalidMessageType(msg_type)),
        }
    }
}

impl BackendMessage {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            BackendMessage::AuthenticationOk => {
                buf.push(b'R');
                buf.extend_from_slice(&8_i32.to_be_bytes()); // length
                buf.extend_from_slice(&0_i32.to_be_bytes()); // auth OK
            }
            BackendMessage::AuthenticationMD5Password { salt } => {
                buf.push(b'R');
                buf.extend_from_slice(&12_i32.to_be_bytes()); // length
                buf.extend_from_slice(&5_i32.to_be_bytes());  // MD5
                buf.extend_from_slice(salt);
            }
            BackendMessage::ParameterStatus { name, value } => {
                buf.push(b'S');
                let body_len = name.len() + 1 + value.len() + 1;
                buf.extend_from_slice(&((4 + body_len) as i32).to_be_bytes());
                buf.extend_from_slice(name.as_bytes());
                buf.push(0);
                buf.extend_from_slice(value.as_bytes());
                buf.push(0);
            }
            BackendMessage::BackendKeyData { process_id, secret_key } => {
                buf.push(b'K');
                buf.extend_from_slice(&12_i32.to_be_bytes()); // length
                buf.extend_from_slice(&process_id.to_be_bytes());
                buf.extend_from_slice(&secret_key.to_be_bytes());
            }
            BackendMessage::ReadyForQuery { status } => {
                buf.push(b'Z');
                buf.extend_from_slice(&5_i32.to_be_bytes());
                buf.push(*status);
            }
            BackendMessage::RowDescription { fields } => {
                buf.push(b'T');
                let mut body = Vec::new();
                body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
                for field in fields {
                    body.extend_from_slice(field.name.as_bytes());
                    body.push(0);
                    body.extend_from_slice(&field.table_oid.to_be_bytes());
                    body.extend_from_slice(&field.column_attr.to_be_bytes());
                    body.extend_from_slice(&field.type_oid.to_be_bytes());
                    body.extend_from_slice(&field.type_size.to_be_bytes());
                    body.extend_from_slice(&field.type_modifier.to_be_bytes());
                    body.extend_from_slice(&field.format.to_be_bytes());
                }
                buf.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
                buf.extend_from_slice(&body);
            }
            BackendMessage::DataRow { values } => {
                buf.push(b'D');
                let mut body = Vec::new();
                body.extend_from_slice(&(values.len() as i16).to_be_bytes());
                for val in values {
                    match val {
                        Some(data) => {
                            body.extend_from_slice(&(data.len() as i32).to_be_bytes());
                            body.extend_from_slice(data);
                        }
                        None => {
                            body.extend_from_slice(&(-1_i32).to_be_bytes());
                        }
                    }
                }
                buf.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
                buf.extend_from_slice(&body);
            }
            BackendMessage::CommandComplete { tag } => {
                buf.push(b'C');
                let body_len = tag.len() + 1;
                buf.extend_from_slice(&((4 + body_len) as i32).to_be_bytes());
                buf.extend_from_slice(tag.as_bytes());
                buf.push(0);
            }
            BackendMessage::ErrorResponse { severity, code, message } => {
                buf.push(b'E');
                let mut body = Vec::new();
                body.push(b'S');
                body.extend_from_slice(severity.as_bytes());
                body.push(0);
                body.push(b'C');
                body.extend_from_slice(code.as_bytes());
                body.push(0);
                body.push(b'M');
                body.extend_from_slice(message.as_bytes());
                body.push(0);
                body.push(0); // terminator
                buf.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
                buf.extend_from_slice(&body);
            }
            BackendMessage::ParseComplete => {
                buf.push(b'1');
                buf.extend_from_slice(&4_i32.to_be_bytes());
            }
            BackendMessage::BindComplete => {
                buf.push(b'2');
                buf.extend_from_slice(&4_i32.to_be_bytes());
            }
            BackendMessage::NoData => {
                buf.push(b'n');
                buf.extend_from_slice(&4_i32.to_be_bytes());
            }
            BackendMessage::EmptyQueryResponse => {
                buf.push(b'I');
                buf.extend_from_slice(&4_i32.to_be_bytes());
            }
            BackendMessage::ParameterDescription { param_types } => {
                buf.push(b't');
                let mut body = Vec::new();
                body.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
                for oid in param_types {
                    body.extend_from_slice(&oid.to_be_bytes());
                }
                buf.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
                buf.extend_from_slice(&body);
            }
            BackendMessage::CopyInResponse { format, column_formats } => {
                buf.push(b'G');
                let mut body = Vec::new();
                body.push(*format);
                body.extend_from_slice(&(column_formats.len() as i16).to_be_bytes());
                for fmt in column_formats {
                    body.extend_from_slice(&fmt.to_be_bytes());
                }
                buf.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
                buf.extend_from_slice(&body);
            }
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    // ========================================================================
    // STARTUP MESSAGE TESTS
    // ========================================================================

    #[test]
    fn test_parse_startup_message() {
        // PostgreSQL startup message format:
        // Int32: length (including self)
        // Int32: protocol version (196608 = 3.0)
        // String pairs: key\0value\0 ... \0
        let mut buf = BytesMut::new();
        let params = vec![("user", "testuser"), ("database", "testdb")];

        // Build the body first to compute length
        let mut body = BytesMut::new();
        body.put_i32(196608); // protocol 3.0
        for (k, v) in &params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminator

        buf.put_i32((body.len() + 4) as i32); // length includes itself
        buf.extend_from_slice(&body);

        let msg = FrontendMessage::parse_startup(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Startup { version, params } => {
                assert_eq!(version, 196608);
                assert_eq!(params.get("user").unwrap(), "testuser");
                assert_eq!(params.get("database").unwrap(), "testdb");
            }
            _ => panic!("Expected Startup message"),
        }
    }

    #[test]
    fn test_parse_ssl_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(8); // length
        buf.put_i32(80877103); // SSL request code
        let msg = FrontendMessage::parse_startup(&mut buf.freeze()).unwrap();
        assert!(matches!(msg, FrontendMessage::SslRequest));
    }

    // ========================================================================
    // QUERY MESSAGE TESTS
    // ========================================================================

    #[test]
    fn test_parse_query_message() {
        // Query message: 'Q', Int32 length, String query\0
        let query = "SELECT 1";
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + query.len() + 1) as i32);
        buf.put_slice(query.as_bytes());
        buf.put_u8(0);

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Query(q) => assert_eq!(q, "SELECT 1"),
            _ => panic!("Expected Query message"),
        }
    }

    // ========================================================================
    // PARSE MESSAGE TESTS (Extended Query Protocol)
    // ========================================================================

    #[test]
    fn test_parse_parse_message() {
        // Parse message: 'P', Int32 length, String name\0, String query\0, Int16 num_params, [Int32 param_oid]*
        let name = "stmt1";
        let query = "SELECT $1::int";
        let mut buf = BytesMut::new();
        buf.put_u8(b'P');
        let body_len = name.len() + 1 + query.len() + 1 + 2 + 4; // one param OID
        buf.put_i32((4 + body_len) as i32);
        buf.put_slice(name.as_bytes());
        buf.put_u8(0);
        buf.put_slice(query.as_bytes());
        buf.put_u8(0);
        buf.put_i16(1); // 1 param
        buf.put_i32(23); // INT4 OID

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Parse { name, query, param_types } => {
                assert_eq!(name, "stmt1");
                assert_eq!(query, "SELECT $1::int");
                assert_eq!(param_types, vec![23]);
            }
            _ => panic!("Expected Parse message"),
        }
    }

    // ========================================================================
    // BIND MESSAGE TESTS
    // ========================================================================

    #[test]
    fn test_parse_bind_message() {
        // Bind: 'B', Int32 len, String portal\0, String stmt\0,
        //   Int16 num_format_codes, [Int16 format_code]*,
        //   Int16 num_params, [Int32 param_len, Byte* param_value]*,
        //   Int16 num_result_format_codes, [Int16 format_code]*
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');

        let mut body = BytesMut::new();
        body.put_slice(b"\0");       // portal name (empty = unnamed)
        body.put_slice(b"stmt1\0"); // statement name
        body.put_i16(0);             // 0 format codes (all text)
        body.put_i16(1);             // 1 parameter
        let val = b"42";
        body.put_i32(val.len() as i32);
        body.put_slice(val);
        body.put_i16(0);             // 0 result format codes (all text)

        buf.put_i32((4 + body.len()) as i32);
        buf.extend_from_slice(&body);

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Bind { portal, statement, param_formats, params, result_formats } => {
                assert_eq!(portal, "");
                assert_eq!(statement, "stmt1");
                assert_eq!(param_formats, vec![]);
                assert_eq!(params, vec![Some(b"42".to_vec())]);
                assert_eq!(result_formats, vec![]);
            }
            _ => panic!("Expected Bind message"),
        }
    }

    // ========================================================================
    // DESCRIBE / EXECUTE / SYNC / TERMINATE TESTS
    // ========================================================================

    #[test]
    fn test_parse_describe_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        buf.put_i32(4 + 1 + 1); // len + type byte + null terminator
        buf.put_u8(b'S'); // describe Statement
        buf.put_u8(0);    // unnamed

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Describe { kind, name } => {
                assert_eq!(kind, DescribeKind::Statement);
                assert_eq!(name, "");
            }
            _ => panic!("Expected Describe message"),
        }
    }

    #[test]
    fn test_parse_execute_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        buf.put_i32(4 + 1 + 4); // len + null terminator + max_rows
        buf.put_u8(0); // unnamed portal
        buf.put_i32(0); // 0 = no limit

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        match msg {
            FrontendMessage::Execute { portal, max_rows } => {
                assert_eq!(portal, "");
                assert_eq!(max_rows, 0);
            }
            _ => panic!("Expected Execute message"),
        }
    }

    #[test]
    fn test_parse_sync_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        buf.put_i32(4);

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        assert!(matches!(msg, FrontendMessage::Sync));
    }

    #[test]
    fn test_parse_terminate_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'X');
        buf.put_i32(4);

        let msg = FrontendMessage::parse(&mut buf.freeze()).unwrap();
        assert!(matches!(msg, FrontendMessage::Terminate));
    }

    // ========================================================================
    // BACKEND (SERVER → CLIENT) MESSAGE SERIALIZATION TESTS
    // ========================================================================

    #[test]
    fn test_serialize_authentication_ok() {
        let msg = BackendMessage::AuthenticationOk;
        let bytes = msg.serialize();
        // R, Int32(8), Int32(0)
        assert_eq!(bytes[0], b'R');
        assert_eq!(&bytes[1..5], &8_i32.to_be_bytes());
        assert_eq!(&bytes[5..9], &0_i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_authentication_md5() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let msg = BackendMessage::AuthenticationMD5Password { salt };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'R');
        assert_eq!(&bytes[1..5], &12_i32.to_be_bytes()); // length = 4 + 4 + 4
        assert_eq!(&bytes[5..9], &5_i32.to_be_bytes());  // MD5 auth type = 5
        assert_eq!(&bytes[9..13], &salt);
    }

    #[test]
    fn test_serialize_parameter_status() {
        let msg = BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "15.0".to_string(),
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'S');
        // Should contain null-terminated name and value
        let body = &bytes[5..];
        assert!(body.starts_with(b"server_version\0"));
        assert!(body.ends_with(b"15.0\0"));
    }

    #[test]
    fn test_serialize_ready_for_query() {
        let msg = BackendMessage::ReadyForQuery { status: b'I' };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'Z');
        assert_eq!(&bytes[1..5], &5_i32.to_be_bytes());
        assert_eq!(bytes[5], b'I');
    }

    #[test]
    fn test_serialize_row_description() {
        let msg = BackendMessage::RowDescription {
            fields: vec![
                FieldDescription {
                    name: "id".to_string(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 23,    // INT4
                    type_size: 4,
                    type_modifier: -1,
                    format: 0,       // text
                },
                FieldDescription {
                    name: "name".to_string(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25,    // TEXT
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                },
            ],
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'T');
        // After the header, Int16 field count = 2
        assert_eq!(&bytes[5..7], &2_i16.to_be_bytes());
    }

    #[test]
    fn test_serialize_data_row() {
        let msg = BackendMessage::DataRow {
            values: vec![
                Some(b"1".to_vec()),
                Some(b"hello".to_vec()),
                None,
            ],
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'D');
        // field count
        assert_eq!(&bytes[5..7], &3_i16.to_be_bytes());
        // first value: length 1, data "1"
        assert_eq!(&bytes[7..11], &1_i32.to_be_bytes());
        assert_eq!(bytes[11], b'1');
        // second value: length 5, data "hello"
        assert_eq!(&bytes[12..16], &5_i32.to_be_bytes());
        assert_eq!(&bytes[16..21], b"hello");
        // third value: NULL = -1
        assert_eq!(&bytes[21..25], &(-1_i32).to_be_bytes());
    }

    #[test]
    fn test_serialize_command_complete() {
        let msg = BackendMessage::CommandComplete {
            tag: "SELECT 1".to_string(),
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'C');
        let body = &bytes[5..];
        assert_eq!(body, b"SELECT 1\0");
    }

    #[test]
    fn test_serialize_error_response() {
        let msg = BackendMessage::ErrorResponse {
            severity: "ERROR".to_string(),
            code: "42601".to_string(),
            message: "syntax error".to_string(),
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'E');
        // Body should contain: S severity\0 C code\0 M message\0 \0
        let body = &bytes[5..];
        assert!(body.starts_with(b"S"));
        assert!(body.ends_with(&[0]));
    }

    #[test]
    fn test_serialize_parse_complete() {
        let msg = BackendMessage::ParseComplete;
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'1');
        assert_eq!(&bytes[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_bind_complete() {
        let msg = BackendMessage::BindComplete;
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'2');
        assert_eq!(&bytes[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_no_data() {
        let msg = BackendMessage::NoData;
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'n');
        assert_eq!(&bytes[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_empty_query_response() {
        let msg = BackendMessage::EmptyQueryResponse;
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'I');
        assert_eq!(&bytes[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_parameter_description() {
        let msg = BackendMessage::ParameterDescription {
            param_types: vec![23, 25], // INT4, TEXT
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b't');
        assert_eq!(&bytes[5..7], &2_i16.to_be_bytes());
        assert_eq!(&bytes[7..11], &23_i32.to_be_bytes());
        assert_eq!(&bytes[11..15], &25_i32.to_be_bytes());
    }

    // ========================================================================
    // ROUND-TRIP TESTS
    // ========================================================================

    #[test]
    fn test_backend_key_data() {
        let msg = BackendMessage::BackendKeyData {
            process_id: 12345,
            secret_key: 67890,
        };
        let bytes = msg.serialize();
        assert_eq!(bytes[0], b'K');
        assert_eq!(&bytes[1..5], &12_i32.to_be_bytes()); // length
        assert_eq!(&bytes[5..9], &12345_i32.to_be_bytes());
        assert_eq!(&bytes[9..13], &67890_i32.to_be_bytes());
    }
}
