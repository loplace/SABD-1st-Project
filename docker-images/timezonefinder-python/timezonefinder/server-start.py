import socket               # Import socket module
from timezonefinder import TimezoneFinder

def retrieveTZ(latitude,longitude):
    tf = TimezoneFinder(in_memory=True)
    tzresult = tf.timezone_at(lng=longitude, lat=latitude)
    return tzresult

soc = socket.socket()         # Create a socket object
host = "0.0.0.0" # Get local machine name
port = 8888                # Reserve a port for your service.
soc.bind((host, port))       # Bind to the port
soc.listen(5)                 # Now wait for client connection.
print("Accepting connection on "+ host+":"+str(port))

while True:
    conn, addr = soc.accept()     # Establish connection with client.
    print ("Got connection from",addr)
    data = conn.recv(1024).decode('utf-8')
    msg = str(data)
    print ("Data from client: " + msg)
    if ( len(msg) > 0 ):
        pair = msg.split(";")
        #print(pair)
        latitude = float(pair[0])
        longitude = float(pair[1])
        #print("latitude: "+str(latitude))
        #print("longitude: "+str(longitude))
        
        tz = retrieveTZ(latitude,longitude)

        datatosend = tz.encode()
        conn.send(datatosend)
        print("Sent to client: "+tz)
        conn.close()
    else:
        print("Go away")
        conn.close()
