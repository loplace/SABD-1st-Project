package model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

@ToString
public class WeatherMeasurementPojo implements Serializable, CityKey {

    @Getter @Setter
    private String city;

    @Getter @Setter
    private DateTime measuredAt;

    @Getter @Setter
    private double measurementValue = 0.0;

    @Getter @Setter
    private String country;

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);


    public WeatherMeasurementPojo(String city, String measuredAt, double measurementValue) {
        this.city = city;
        this.measuredAt = formatDate(measuredAt);
        this.measurementValue = measurementValue;
    }

    public static DateTime formatDate(String date){

        return DateTime.parse(date,formatter);
    }

}
