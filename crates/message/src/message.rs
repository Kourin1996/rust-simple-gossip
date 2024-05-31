use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub struct Message {
    pub sender: String,
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum MessageBody {
    Handshake,
    DiscoveryRequest,
    DiscoveryResponse { peers: Vec<String> },
    GossipBroadcast { message: String },
}

impl Message {
    pub fn encode(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn decode(msg: String) -> Self {
        serde_json::from_str(msg.as_str()).unwrap()
    }
}
