#!/bin/bash

source config.sh

serverID=S1
serverPort=$server1Port
serverIP=$server1IP
heartbeat=$LFDHeartbeat
GFDIP=$GFDIP
GFDPort=$GFDPort
lfdPort=$LFD1Port

javac --source-path src -d bin src/LocalFaultDetector.java
cd bin
java LocalFaultDetector LFD1 $lfdPort $heartbeat $serverID $serverIP $serverPort $GFDIP $GFDPort