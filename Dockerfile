FROM python:3.8-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip install pandas-gbq -U
RUN pip install -U flask-cors
COPY . .

CMD [ "python3", "app.py"]

