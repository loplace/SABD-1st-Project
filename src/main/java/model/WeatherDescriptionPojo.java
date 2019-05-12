package model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

@ToString
public class WeatherDescriptionPojo implements Serializable, CityKey {

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);

    @Getter @Setter
    private String city;
    @Getter
    private DateTime dateTime;

    @Getter @Setter
    private String weatherCondition;

    @Getter
    private DateTimeZone dateTimezone;

    public WeatherDescriptionPojo(String city, String dateTime, String weatherCondition) {
        this.city = city;
        this.dateTime = formatDate(dateTime);
        this.weatherCondition = weatherCondition;
    }

    public DateTime getLocalDateTime(){
        if (this.dateTimezone!= null) {
            return  this.dateTime.withZone(this.dateTimezone);
        }
        return this.dateTime;
    }

    public void setDateTimezone(String dateTimezone) {
        this.dateTimezone = DateTimeZone.forID(dateTimezone);
    }

    public void setDateTime(String dateTime) {
        this.dateTime = formatDate(dateTime);
    }

    public static DateTime formatDate(String date){
        return DateTime.parse(date,formatter);
    }

}
