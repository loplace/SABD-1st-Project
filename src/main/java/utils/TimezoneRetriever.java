package utils;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Scanner;

public class TimezoneRetriever {

    public static final String HOST = "localhost";
    public static final int PORT = 8888;


    public static String retrieveTimezone(double latitude, double longitude) {
        String msgFromServer = null;
        try{
            Socket soc = new Socket(HOST,PORT);

            BufferedWriter bout = new BufferedWriter(new OutputStreamWriter( soc.getOutputStream()) );
            Scanner scan = new Scanner(soc.getInputStream());

            String dataToSend = latitude + ";"+longitude;
            bout.write(dataToSend);
            bout.flush();

            if (scan.hasNext()) {
                msgFromServer = scan.nextLine();
            }

            bout.close();
            scan.close();
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
