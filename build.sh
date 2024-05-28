#!/bin/sh

cargo build --release --package gossip-app
cp target/release/gossip_app ./peer
