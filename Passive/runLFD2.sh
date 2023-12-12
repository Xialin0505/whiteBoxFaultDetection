#!/bin/bash

source config.sh

serverID=S2
serverPort=$server2Port
serverIP=$server2IP
heartbeat=$LFDHeartbeat
GFDIP=$GFDIP
GFDPort=$GFDPort
lfdPort=$LFD2Port

javac --source-path src -d bin src/LocalFaultDetector.java
cd bin
java LocalFaultDetector LFD2 $lfdPort $heartbeat $serverID $serverIP $serverPort $GFDIP $GFDPort