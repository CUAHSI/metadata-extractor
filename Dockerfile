FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

WORKDIR /app

RUN apt-get update
RUN apt-get -y install python3.12
RUN apt-get -y install python3-pip

RUN python3 -m pip config set global.break-system-packages true
RUN pip3 install -U h5py
RUN pip3 install -U setuptools
RUN pip3 install -U hdf5plugin
RUN pip3 install -U netCDF4

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt

COPY hsextract hsextract

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "hsextract/main.py"]