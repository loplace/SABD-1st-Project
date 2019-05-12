package model;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public class WeatherMeasurementPojo implements Serializable, CityKey {

    private String city;
    private DateTime measuredAt;
    private double measurementValue = 0.0;

    private String country;

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);


    public WeatherMeasurementPojo(String city, String measuredAt, double measurementValue) {
        this.city = city;
        this.measuredAt = formatDate(measuredAt);
        this.measurementValue = measurementValue;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public DateTime getMeasuredAt() {
        return measuredAt;
    }

    public void setMeasuredAt(DateTime measuredAt) {
        this.measuredAt = measuredAt;
    }

    public double getMeasurementValue() {
        return measurementValue;
    }

    public void setMeasurementValue(double measurementValue) {
        this.measurementValue = measurementValue;
    }

    public static DateTime formatDate (String date){

        return DateTime.parse(date,formatter);
    }

    @Override
    public String toString() {
        return "WeatherMeasurementPojo{" +
                "city='" + city + '\'' +
                ", measuredAt=" + measuredAt +
                ", measurementValue=" + measurementValue +
                '}';
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCountry() {
        return country;
    }
}
