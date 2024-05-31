#!/bin/sh

./peer --period=6 --port=8081 --connect="127.0.0.1:8080" | tee output2.txt