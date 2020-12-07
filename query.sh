#!/bin/bash
docker-compose run -v `pwd`/data/bitcoin:/coinblas/blocks -v `pwd`:/coinblas -e GOOGLE_APPLICATION_CREDENTIALS=pgsodium-e46130a430c2.json builder ipython -i -m coinblas.bitcoin $*

