pub struct GossipApp {
    port: u16,
    broadcast_period: u32,
    initial_peers: Vec<String>,
}

impl GossipApp {
    pub fn new(port: u16, broadcast_period: u32, initial_peers: Vec<String>) -> Self {
        GossipApp {
            port,
            broadcast_period,
            initial_peers,
        }
    }

    pub fn run(&self) {
        tracing::debug!(
            "Gossip Server starting, port={}, period={}, initial peers=[{}]",
            self.port,
            self.broadcast_period,
            self.initial_peers.join(", ")
        );
    }
}
