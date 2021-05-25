ARG BASE_CONTAINER=graphblas/pygraphblas-notebook:v4.0.3
FROM ${BASE_CONTAINER}
ADD . /home/jovyan/coinblas
USER root
RUN pip3 install -r /home/jovyan/coinblas/requirements.txt
RUN python3 /home/jovyan/setup.py develop


