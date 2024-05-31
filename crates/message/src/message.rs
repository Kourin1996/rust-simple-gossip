use serde::{Deserialize, Serialize};

// Message is a struct that represents a message in the application
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub struct Message {
    // sender is the address of the sender
    pub sender: String,
    // message body
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum MessageBody {
    // Handshake is a message to tell the receiver the sender's address
    Handshake,
    // DiscoveryRequest is a message to request the receiver's peers
    DiscoveryRequest,
    // DiscoveryResponse is a message to respond to a peer discovery request
    DiscoveryResponse { peers: Vec<String> },
    // GossipBroadcast is a message to broadcast a gossip message
    GossipBroadcast { message: String },
}

impl Message {
    // encode serializes the message to a JSON string
    pub fn encode(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    // decode deserializes the message from a JSON string
    pub fn decode(msg: String) -> Self {
        serde_json::from_str(msg.as_str()).unwrap()
    }
}
