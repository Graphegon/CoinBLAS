#!/bin/bash
docker-compose run -w /home/jovyan/coinblas coinblas python -m coinblas.bitcoin $*


