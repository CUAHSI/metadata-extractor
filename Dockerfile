FROM osgeo/gdal:ubuntu-small-3.2.0

WORKDIR /app

RUN apt-get update
RUN apt-get -y install python3.9
RUN apt-get -y install python3-pip

RUN pip3 install -U setuptools
RUN pip3 install -U h5py
RUN pip3 install -U hdf5plugin
RUN pip3 install -U numpy
RUN pip3 install -U netCDF4

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt

COPY hsextract hsextract

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "hsextract/main.py"]