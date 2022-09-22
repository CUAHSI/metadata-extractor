FROM osgeo/gdal:ubuntu-small-3.2.0

WORKDIR /app

RUN apt-get update
RUN apt-get install python3
RUN apt-get -y install python3-pip

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm requirements.txt

COPY hsextract hsextract

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "hsextract/main.py"]