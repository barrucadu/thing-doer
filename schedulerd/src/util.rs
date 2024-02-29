use serde_json::Value;

/// Parse some bytes into json.
pub fn bytes_to_json(bytes: Vec<u8>) -> Option<Value> {
    if let Ok(json) = String::from_utf8(bytes) {
        serde_json::from_str::<Value>(&json).ok()
    } else {
        None
    }
}
