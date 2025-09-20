FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4

WORKDIR /app

RUN apt-get update
RUN apt-get -y install python3.12
RUN apt-get -y install python3-pip

RUN python3 -m pip config set global.break-system-packages true
RUN pip3 install -U h5py
RUN pip3 install -U setuptools
RUN pip3 install -U h5netcdf==1.6.4
RUN pip3 install -U netCDF4==1.7.2

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt
RUN pip3 install uvicorn
RUN pip3 install fastapi

RUN apt-get update
RUN apt-get -y upgrade
RUN curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash
RUN apt install -y redpanda-rpk redpanda-connect
RUN pip3 install redpanda-connect

COPY hsextract hsextract
COPY hsextract_dbos hsextract_dbos
#COPY pipelines pipelines

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["rpk", "connect", "run", "--rpc-plugins=/app/hsextract_dbos/app/extract_metadata_processor.yaml", "/app/hsextract_dbos/app/extract_metadata.yaml"]