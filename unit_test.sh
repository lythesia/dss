#!/bin/bash

TEST=$1
N=$2
LOG=$HOME/$TEST.log

for i in `seq 1 $N`; do
  echo "$TEST: attempt $i"
  RUST_BACKTRACE=1 RUST_LOG=raft=debug cargo test -p raft -- --nocapture --test $TEST &> $LOG
  if [ $? -eq 0 ]; then
    echo "pass"
  else
    echo "fail"
    exit 1
  fi
done
exit 0
