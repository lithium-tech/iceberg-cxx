#!/bin/bash

nohup java -jar /nessie/nessie-quarkus-0.103.0-runner.jar > nessie.log 2>&1 &
sleep 20
