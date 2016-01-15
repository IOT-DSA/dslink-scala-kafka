#!/usr/bin/env bash
sbt clean compile dist
cp target/universal/dslink-scala-kafka-0.1.0-SNAPSHOT.zip ../../files/dslink-scala-kafka.zip
