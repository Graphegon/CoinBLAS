#!/bin/bash
docker run -u $(id -u ${USER}):$(id -g ${USER}) -v `pwd`/data/bitcoin:/coinblas/blocks -v `pwd`:/coinblas -e GOOGLE_APPLICATION_CREDENTIALS=pgsodium-e46130a430c2.json -e HOME=/coinblas -v`pwd`.ipython.history:/coinblas/.ipython/profile_default/history.sqlite -it coinblas ipython $*

