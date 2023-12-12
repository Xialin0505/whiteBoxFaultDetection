#!/bin/bash

source config.sh

serverPort=$server1Port
serverIP=$server1IP

javac --source-path src -d bin src/Server.java
cd bin
java Server S1 $serverIP $serverPort
