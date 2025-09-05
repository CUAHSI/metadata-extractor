FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

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

COPY hsextract hsextract
COPY hsextract_dbos hsextract_dbos

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["uvicorn", "hsextract_dbos.app.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]