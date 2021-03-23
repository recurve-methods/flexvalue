#!/bin/bash
# first argument is the port to run on 
# if not specified, select a random port to run on
PORT=${1:-`python -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'`}
HOST_PORT_JUPYTER=$PORT docker-compose up jupyter 

