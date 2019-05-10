# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM sabbir1cse/ubuntu-python-pip-supervisor
USER root

# dipendenze di timezonefinder
RUN pip3 install numpy importlib_resources 
RUN pip3 install geopy 

RUN mkdir /timezonefinder
COPY timezonefinder /timezonefinder

CMD python3 /timezonefinder/server-start.py

EXPOSE 8888
