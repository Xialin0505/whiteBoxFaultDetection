#!/bin/bash

source config.sh

serverID=GFD
serverPort=$GFDPort
serverIP=$GFDIP
heartbeat=$GFDHeartbeat

javac --source-path src -d bin src/GlobalFaultDetector.java
cd bin
java GlobalFaultDetector $heartbeat $serverID $serverIP $serverPort $RMIP $RMPort