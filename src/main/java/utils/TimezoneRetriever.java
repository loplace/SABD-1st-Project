package utils;

import net.xdevelop.jpclient.PyResult;
import net.xdevelop.jpclient.PyServeContext;
import net.xdevelop.jpclient.PyServeException;

public class TimezoneRetriever {

    public static String retrieveTimezone(double latitude, double longitude) throws PyServeException {

        PyServeContext.init("localhost",8888);
        String script = "from timezonefinder import TimezoneFinder\n" +
                "\n" +
              //  "tf = TimezoneFinder(in_memory=True)\n" +
                "tf = TimezoneFinder(in_memory=False)\n" +
                "\n" +
                "latitude = " + String.valueOf(latitude)  +"\n" +
                "longitude = " + String.valueOf(longitude)  +"\n" +
                "\n" +
                "_result_ = tf.timezone_at(lng=longitude, lat=latitude)\n";

      //  System.out.println(script);

        // sned the script to PyServe, it returns the final result
        PyResult rs = PyServeContext.getExecutor().exec(script);

        // check if the execution is success
        if (rs.isSuccess()) {
            System.out.println("Result: " + rs.getResult()); // get the _result_ value
        }
        else {
            System.out.println("Execute python script failed: " + rs.getMsg());
        }

        return rs.getResult().replaceAll("\"","");

    }

    public static void main(String[] args) throws PyServeException {
        double latitude = 45.523449;
        double longitude = -122.676208;

        String tz = TimezoneRetriever.retrieveTimezone(latitude,longitude);
        System.out.println(tz);
    }

}
