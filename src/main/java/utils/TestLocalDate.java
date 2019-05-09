package utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TestLocalDate {

    private static DateTimeFormatter UTCformatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);


    public static void main(String[] args) {

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

        double latitude = 37.774929;
        double longitude = -122.419418;

        String timezone = CityLocationRetriever.retrieveLocationInfo(latitude, longitude);
        System.out.println("Timezone from server: "+timezone);
    }
}
