#!/bin/bash

pip3 install --upgrade --break-system-packages pynessie "marshmallow<4" flask
wget --no-verbose https://github.com/projectnessie/nessie/releases/download/nessie-0.104.1/nessie-quarkus-0.104.1-runner.jar
nohup java -jar nessie-quarkus-0.104.1-runner.jar > nessie.log 2>&1 &
sleep 20
