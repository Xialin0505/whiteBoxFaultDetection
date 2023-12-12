#!/bin/bash

source config.sh

javac --source-path src -d bin src/ReplicationManager.java
cd bin
java ReplicationManager RM $RMIP $RMPort