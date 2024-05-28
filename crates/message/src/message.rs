use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub struct Message {
    pub sender: String,
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MessageBody {
    Handshake,
    DiscoveryRequest,
    DiscoveryResponse { peers: Vec<String> },
    GossipBroadcast { message: String },
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let encoded = serde_json::to_string(&self).unwrap();
        tracing::debug!("Encoding message: {:?}", encoded);
        encoded.into_bytes()
    }

    pub fn decode(data: &[u8]) -> Self {
        tracing::debug!("Decoding message: {:?}", String::from_utf8_lossy(data));
        serde_json::from_slice(data).unwrap()
    }
}
