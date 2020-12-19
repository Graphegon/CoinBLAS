#!/bin/bash
docker-compose run -w /home/jovyan/coinblas builder pytest $* tests/

