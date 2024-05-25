#!/bin/sh

cargo build --release --package gossip_app
cp target/release/gossip_app ./peer
