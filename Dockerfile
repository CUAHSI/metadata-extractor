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

RUN echo Wa1r4XOFeTkxPUim:iweANBKNCuIMMJwgrX02LPxBbb85hQGD > /etc/s3cred
RUN chmod 600 /etc/s3cred

RUN mkdir /s3
# TODO pull out bucket name, minio server key/token to be passed in later
#RUN s3fs demo-composite /s3 -o passwd_file=/etc/s3cred,use_path_request_style,url=https://play.min.io:9000

ENTRYPOINT ["python", "hsextract/main.py"] 