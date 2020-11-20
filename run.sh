#!/bin/bash
docker run  -v `pwd`/data/bitcoin:/coinblas/blocks -v `pwd`:/coinblas -e GOOGLE_APPLICATION_CREDENTIALS=pgsodium-e46130a430c2.json -it coinblas ipython -i -m coinblas $1

