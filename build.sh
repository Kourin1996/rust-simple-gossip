#!/bin/sh

cargo build --release
cp target/release/peer .
