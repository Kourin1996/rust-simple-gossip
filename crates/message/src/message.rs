use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MessageBody {
    Handshake { sender: String },
    DiscoveryRequest { sender: String },
    DiscoveryResponse { sender: String, peers: Vec<String> },
    GossipBroadcast { sender: String, message: String },
}

impl MessageBody {
    pub fn sender(&self) -> String {
        match self {
            MessageBody::Handshake { sender } => sender.clone(),
            MessageBody::DiscoveryRequest { sender } => sender.clone(),
            MessageBody::DiscoveryResponse { sender, .. } => sender.clone(),
            MessageBody::GossipBroadcast { sender, .. } => sender.clone(),
        }
    }

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
