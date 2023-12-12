#!/bin/bash

source config.sh

serverPort=$server1Port,$server2Port,$server3Port
serverIP=$server1IP,$server2IP,$server3IP

javac --source-path src -d bin src/Client.java
cd bin
java Client C2 S1,S2,S3 $serverIP $serverPort