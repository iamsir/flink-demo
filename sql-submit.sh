#!/usr/bin/env bash

FLINK_DIR=/local/sda/flink/flink-1.9.1

$FLINK_DIR/bin/flink run -d -p 4 -c com.zhdan.flink.cli.SqlSubmit target/flink-demo-1.0-SNAPSHOT.jar $1