#!/bin/bash

case $1 in
  "3c")
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
    ;;
  "3d")
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
    ;;
  "3b")
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 5 --time-limit 20 --rate 10
    ;;
  "3e")
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
    ;;
  *)
    ../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/broadcast --node-count 1 --time-limit 20 --rate 10
    ;;
esac
