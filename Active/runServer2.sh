#!/bin/bash

source config.sh

serverPort=$server2Port
serverIP=$server2IP

javac --source-path src -d bin src/Server.java
cd bin
java Server S2 $serverIP $serverPort
