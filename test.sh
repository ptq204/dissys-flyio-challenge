#!/bin/bash

case $1 in
  "2")
    cd unique-id-generator && go install
    ../maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/unique-id-generator --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
    ;;
  "3a")
    cd broadcast && go install
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 1 --time-limit 20 --rate 10
    ;;
  "3c")
    cd broadcast && go install
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
    ;;
  "3b")
    cd broadcast && go install
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 5 --time-limit 20 --rate 10
    ;;
  *)
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 1 --time-limit 20 --rate 10
    ;;
esac