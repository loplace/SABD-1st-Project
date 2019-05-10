# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM ubuntu:xenial
USER root

# dipendenze di timezonefinder
RUN apt-get update
RUN apt-get -y install python3-pip
RUN pip3 install numpy importlib_resources 
RUN pip3 install geopy 

RUN mkdir /timezonefinder
COPY timezonefinder /timezonefinder

CMD python3 /timezonefinder/server-start.py

EXPOSE 8888
