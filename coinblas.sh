#!/bin/bash
docker-compose run -w /home/jovyan/coinblas builder python -i -m coinblas.bitcoin $*


