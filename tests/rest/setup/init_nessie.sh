#!/bin/bash

nohup java -jar nessie-quarkus-0.104.1-runner.jar > nessie.log 2>&1 &
sleep 20
