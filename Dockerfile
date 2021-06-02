ARG BASE_CONTAINER=graphblas/pygraphblas-notebook:v4.0.3
FROM ${BASE_CONTAINER}
ADD . /home/jovyan/coinblas
WORKDIR /home/jovyan/coinblas
USER root
RUN pip3 install --upgrade -r /home/jovyan/coinblas/requirements.txt
RUN python3 setup.py develop
