# rust-simple-gossip

A simple example of a gossip protocol utilizing Rust.

## Requirements

- Rust 1.75.0 or later
- Cargo

## Installation

Clone the repository and build the project

```bash
$ git clone https://github.com/Kourin1996/rust-gossip-app.git
$ cd rust-gossip-app

$ cargo build
```

## Usage

```bash
$ ./build.sh

$ ./peer --port 8080 --period 5
$ ./peer --period=6 --port=8081 --connect="127.0.0.1:8080"
```

### Example of output

#### peer1

```bash
$ ./peer --period=5 --port=8080

# 00:00:00 - My address is "127.0.0.1:8080"
# 00:00:02 - Connected to peer at "127.0.0.1:8081"
# 00:00:04 - Connected to peer at "127.0.0.1:8082"
# 00:00:05 - Sending message [1aafChbXC8] to ["127.0.0.1:8082", "127.0.0.1:8081"]
# 00:00:08 - Received message [h4HMReEXvl] from "127.0.0.1:8081"
# 00:00:10 - Sending message [1yamAnnjQ0] to ["127.0.0.1:8082", "127.0.0.1:8081"]
# 00:00:11 - Received message [QOZPnl3Cle] from "127.0.0.1:8082"
```

#### peer2

```bash
$ ./peer --period=6 --port=8081 --connect="127.0.0.1:8080"
# 00:00:00 - My address is "127.0.0.1:8081"
# 00:00:00 - Connected to peer at "127.0.0.1:8080"
# 00:00:02 - Connected to peer at "127.0.0.1:8082"
# 00:00:02 - Received message [1aafChbXC8] from "127.0.0.1:8080"
# 00:00:06 - Sending message [h4HMReEXvl] to ["127.0.0.1:8080", "127.0.0.1:8082"]
# 00:00:07 - Received message [1yamAnnjQ0] from "127.0.0.1:8080"
# 00:00:09 - Received message [QOZPnl3Cle] from "127.0.0.1:8082"
# 00:00:12 - Sending message [nKZGSQ2zY8] to ["127.0.0.1:8080", "127.0.0.1:8082"]
```

#### peer3

```bash
$ ./peer --period=7 --port=8082 --connect="127.0.0.1:8080"
# 00:00:00 - My address is "127.0.0.1:8082"
# 00:00:00 - Connected to peer at "127.0.0.1:8080"
# 00:00:00 - Connected to peer at "127.0.0.1:8081"
# 00:00:00 - Received message [1aafChbXC8] from "127.0.0.1:8080"
# 00:00:03 - Received message [h4HMReEXvl] from "127.0.0.1:8081"
# 00:00:05 - Received message [1yamAnnjQ0] from "127.0.0.1:8080"
# 00:00:07 - Sending message [QOZPnl3Cle] to ["127.0.0.1:8081", "127.0.0.1:8080"]
# 00:00:09 - Received message [nKZGSQ2zY8] from "127.0.0.1:8081"
# 00:00:10 - Received message [eeWG6gugXH] from "127.0.0.1:8080"
```

## Test

```bash
$ cargo test
```

## Project Structures

```bash
.
├ bins
│ └ gossip-app
│ 　 ├ src
│ 　 └ Cargo.toml
└ crates
　 ├ connection-manager
　 │ ├ src
　 │ └ Cargo.toml
　 ├ discovery
　 │ ├ src
　 │ └ Cargo.toml
　 ├ gossip
　 │ ├ src
　 │ └ Cargo.toml
　 ├ message
　 │ ├ src
　 │ └ Cargo.toml
　 └ utils
　 　 ├ src
　 　 └ Cargo.toml
```

- `gossip-app` crate: An entrypoint of the application which is responsible for setting CLI options/logger and starting Gossip application
- `connection-manager` crate: A module to manage TCP connections
  + `ConnectionManager` is responsible for establishing connections and provide connected/disconnected events for other modules
  + `Peer` is a struct representing a connected peer, and it provides methods to send/receive a message from/to the peer
- `discovery` crate: is responsible for discovering peers in the network and connecting new peers. When a node connects to new peer, it sends discovery request message to the peer and the peer returns known peers upon the request
- `gossip` crate: is responsible for orchestrating other modules and implementing the gossip protocol
- `message` crate: defines the message struct and serialization/deserialization methods
- `utils` crate: contains utility function for converting mpsc channel to tokio's mpsc channel
