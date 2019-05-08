package utils;

import net.xdevelop.jpclient.PyResult;
import net.xdevelop.jpclient.PyServeContext;
import net.xdevelop.jpclient.PyServeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TestLocalDate {

    private static DateTimeFormatter UTCformatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);


    public static void main(String[] args) throws PyServeException {

        DateTime UTCTime = DateTime.parse("2019-01-01 08:00:00",UTCformatter);
        DateTime romeTime = UTCTime.withZone(DateTimeZone.forID("Europe/Rome"));
        DateTime nyTime =  UTCTime.withZone(DateTimeZone.forID("America/New_York"));
        DateTime laTime =  UTCTime.withZone(DateTimeZone.forID("America/Los_Angeles"));
        DateTime jerusalemTime = UTCTime.withZone(DateTimeZone.forID("Asia/Jerusalem"));


        System.out.println(UTCTime);
        System.out.println(romeTime);
        System.out.println(nyTime);
        System.out.println(laTime);
        System.out.println(jerusalemTime);


        PyServeContext.init("localhost",8888);
        String script = "from timezonefinder import TimezoneFinder\n" +
                "\n" +
                "tf = TimezoneFinder(in_memory=True)\n" +
                "\n" +
                "latitude = 37.774929\n" +
                "longitude = -122.419418\n" +
                "\n" +
                "_result_ = tf.timezone_at(lng=longitude, lat=latitude)\n";

        // sned the script to PyServe, it returns the final result
        PyResult rs = PyServeContext.getExecutor().exec(script);

        // check if the execution is success
        if (rs.isSuccess()) {
            System.out.println("Result: " + rs.getResult()); // get the _result_ value
        }
        else {
            System.out.println("Execute python script failed: " + rs.getMsg());
        }
    }
}
