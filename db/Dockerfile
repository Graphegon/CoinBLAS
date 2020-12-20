FROM "postgres:13"

RUN apt-get update && apt-get install -yq build-essential make git postgresql-server-dev-13

RUN git clone https://github.com/citusdata/postgresql-hll.git
WORKDIR /postgresql-hll
RUN make install
