#!/bin/bash

source config.sh

serverID=RM
serverPort=$RMPort
serverIP=$RMIP

javac --source-path src -d bin src/ReplicationManager.java
cd bin
java ReplicationManager $serverID $serverIP $serverPort