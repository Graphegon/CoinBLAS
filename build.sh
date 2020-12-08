#!/bin/bash
docker-compose run -w /home/jovyan/coinblas builder ipython -i -m coinblas.bitcoin.loader $*

