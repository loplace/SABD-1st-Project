package utils;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.TimeZone;

public class TimezoneRetriever {

    public static final String HOST = "localhost";
    public static final int PORT = 8888;


    public static String retrieveTimezone(double latitude, double longitude) {
        String msgFromServer = null;
        try{
            Socket soc = new Socket(HOST,PORT);
            DataOutputStream dout = new DataOutputStream(soc.getOutputStream());
            DataInputStream din = new DataInputStream(soc.getInputStream());

            String tosend = latitude + ";"+longitude;
            dout.write(tosend.getBytes());
            dout.flush();

            msgFromServer = din.readLine();

            dout.close();
            din.close();
            soc.close();
        } catch(Exception e){
            e.printStackTrace();
        }

       return msgFromServer;

    }

    public static void main(String[] args){
        double latitude = 45.523449;
        double longitude = -122.676208;

        String tz = TimezoneRetriever.retrieveTimezone(latitude,longitude);
        System.out.println(tz);
    }

}
