#!/bin/bash

source config.sh

serverID=S3
serverPort=$server3Port
serverIP=$server3IP
heartbeat=$LFDHeartbeat
GFDIP=$GFDIP
GFDPort=$GFDPort
lfdPort=$LFD3Port

javac --source-path src -d bin src/LocalFaultDetector.java
cd bin
java LocalFaultDetector LFD3 $lfdPort $heartbeat $serverID $serverIP $serverPort $GFDIP $GFDPort