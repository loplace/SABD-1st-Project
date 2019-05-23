package erreesse.resultsuploader.tabledescriptor;

public class Query3TableDescriptor {

    public static String TABLE_NAME = "query3";
    public static String CITY_INFO_COLUMN_FAMILY = "country_city";
    public static String DATES_2017_COLUMN_FAMILY = "2017_datas";
    public static String DATES_2016_COLUMN_FAMILY = "2016_datas";

    public enum COLUMNS {
        ABS_MEAN_DIFF,
        POS,
    }
    public static String COUNTRY = "country";
    public static String CITY = "city";

    public static String ABS_MEAN_DIFF = "abs_mean_diff";
    public static String POS = "position";
}

