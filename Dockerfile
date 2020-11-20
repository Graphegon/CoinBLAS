ARG BASE_CONTAINER=graphblas/pygraphblas-minimal:4.0.0
FROM ${BASE_CONTAINER}

RUN apt-get update && apt-get install -yq curl libpython3-dev python3-pip libreadline-dev git

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

ADD . /coinblas
WORKDIR /coinblas
RUN pip3 install -r requirements.txt

