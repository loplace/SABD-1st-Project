package utils.locationinfo;

import utils.configuration.AppConfiguration;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Scanner;

public class CityLocationRetriever {

    public static final String HOST = AppConfiguration.getProperty("citylocationhelper.address.ip");
    public static final int PORT = Integer.parseInt(AppConfiguration.getProperty("citylocationhelper.address.port"));


    public static String retrieveLocationInfo(double latitude, double longitude) {
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

        String tz = CityLocationRetriever.retrieveLocationInfo(latitude,longitude);
        System.out.println(tz);
    }

}
