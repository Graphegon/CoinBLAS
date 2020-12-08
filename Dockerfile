ARG BASE_CONTAINER=graphblas/pygraphblas-notebook:v4.0.1Dec7
FROM ${BASE_CONTAINER}
ADD . /home/jovyan/coinblas
RUN pip3 install -r /home/jovyan/coinblas/requirements.txt
