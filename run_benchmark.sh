#!/bin/bash
# Run benchmark with Java configured

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

python benchmark_simple.py
