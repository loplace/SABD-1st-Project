package POJO;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public class WeatherDescriptionPojo implements Serializable {

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    private String city;
    private DateTime dateTime;
    private String weatherCondition;

    public WeatherDescriptionPojo(String city, String dateTime, String weatherCondition) {
        this.city = city;
        this.dateTime = formatDate(dateTime);
        this.weatherCondition = weatherCondition;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public DateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = formatDate(dateTime);
    }

    public String getWeatherCondition() {
        return weatherCondition;
    }

    public void setWeatherCondition(String weatherCondition) {
        this.weatherCondition = weatherCondition;
    }

    public static DateTime formatDate (String date){

                return DateTime.parse(date,formatter);
    }

    @Override
    public String toString() {
        return "WeatherDescriptionPojo{" +
                "city='" + city + '\'' +
                ", dateTime=" + dateTime +
                ", weatherCondition='" + weatherCondition + '\'' +
                '}';
    }
}
