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
    pub fn encode(&self) -> String {
        let encoded = serde_json::to_string(&self).unwrap();
        tracing::debug!("Encoding message: {:?}", encoded);
        encoded
    }

    pub fn decode(msg: String) -> Self {
        tracing::debug!("Decoding message: {:?}", msg);
        serde_json::from_str(msg.as_str()).unwrap()
    }
}
