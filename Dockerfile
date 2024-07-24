FROM osgeo/gdal:ubuntu-small-3.2.0

WORKDIR /app

RUN apt-get update
RUN apt-get -y install python3.9
RUN apt-get -y install python3-pip

RUN pip3 install -U "setuptools<66.0.0"
RUN pip3 install -U h5py
RUN pip3 install -U hdf5plugin
RUN pip3 install -U numpy
RUN pip3 install -U netCDF4==1.6.2

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt

RUN pip3 install -U pydantic[email]==1.10

COPY hsextract hsextract

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "hsextract/main.py"]