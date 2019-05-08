# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM sabbir1cse/ubuntu-python-pip-supervisor
USER root

# dipendenze di timezonefinder
RUN pip3 install numpy importlib_resources 

RUN pip3 install jpserve

RUN mkdir /jpserve-timezonefinder
COPY jpserve /jpserve-timezonefinder

EXPOSE 8888
CMD python3 /jpserve-timezonefinder/jpserve-start.py