import socket   
import os            # Import socket module
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

tz_host = os.getenv('TZ_HOST', "0.0.0.0")
tz_port = os.getenv('TZ_PORT', 8888)

def retrieveTZ(latitude,longitude):
    tf = TimezoneFinder(in_memory=True)
    tzresult = tf.timezone_at(lng=longitude, lat=latitude)
    return tzresult

def retrieveNation(latitude,longitude):
    geolocator = Nominatim()
    param = str(latitude)+","+ str(longitude)
    location = geolocator.reverse(param,language = 'en')
    rawString = location.address
    tokens = rawString.split(",")
    nation = tokens[-1].strip()
    return nation


def create_socket():   
    soc = socket.socket()         # Create a socket object
    soc.bind((tz_host, int(tz_port)))       # Bind to the port
    soc.listen(5)                 # Now wait for client connection.
    print("Accepting TimeZoneFinder connection on "+ tz_host+":"+str(tz_port))
    return soc

def receive_data(conn):
    data = conn.recv(1024).decode('utf-8')
    msg = str(data)
    print ("LatLon data from client: " + msg)
    return msg

def send_data(conn,msg):
    datatosend = msg.encode()
    conn.send(datatosend)
    print("TimeZone sent to client: "+msg)

def has_data(data):
    return len(data)>0

def extract_lat_lng(msg):
    pair = msg.split(";")
    #print(pair)
    latitude = float(pair[0])
    longitude = float(pair[1])
    #print("latitude: "+str(latitude))
    #print("longitude: "+str(longitude))
    return latitude,longitude

#if __name__ == "__main__":
soc = create_socket()
while True:
    conn, addr = soc.accept()     # Establish connection with client.
    print ("Got connection from",addr)
    msg = receive_data(conn)
    if ( has_data(msg) ):
        latitude, longitude = extract_lat_lng(msg)
        tz = retrieveTZ(latitude,longitude)
        nat= retrieveNation(latitude,longitude)
        datatosend = str(tz+";"+nat)
        send_data(conn,datatosend)
        conn.close()
    else:
        print("Malformed data")
        conn.close()
