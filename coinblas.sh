#!/bin/bash
docker-compose run -w /home/jovyan/coinblas builder python -m coinblas.bitcoin $*


