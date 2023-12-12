#!/bin/bash

source config.sh

serverPort=$server3Port
serverIP=$server3IP

javac --source-path src -d bin src/Server.java
cd bin
java Server S3 $serverIP $serverPort
