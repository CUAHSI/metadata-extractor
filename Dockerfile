FROM osgeo/gdal:ubuntu-small-3.5.3

WORKDIR /app

RUN apt-get update
RUN apt-get -y install python3.9
RUN apt-get -y install python3-pip

RUN apt-get -y install s3fs

RUN pip3 install -U setuptools
RUN pip3 install -U h5py
RUN pip3 install -U hdf5plugin
RUN pip3 install -U numpy
RUN pip3 install -U netCDF4
RUN pip3 install -U minio

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt

COPY hsextract hsextract

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ARG MNT_POINT=s3
RUN mkdir -p "$MNT_POINT"

COPY run.sh run.sh
CMD ./run.sh

ENTRYPOINT ["python", "hsextract/main.py"] 