package erreesse.hbase.tabledescriptor;

public class Query2TableDescriptor {

    public static String TABLE_NAME = "query2";
    public static String COUNTRY_INFO = "country_info";

    public static String TEMPERATURE_COLUMN_FAMILY = "temperature";
    public static String PRESSURE_COLUMN_FAMILY = "pressure";
    public static String HUMIDITY_COLUMN_FAMILY = "humidity";

    public enum COLUMNS {
        MEAN,
        DEVSTD,
        MIN,
        MAX
    }
    public static String MEAN = "mean";
    public static String DEVSTD = "devstd";
    public static String MIN = "min";
    public static String MAX = "max";


    public static String COUNTRY = "country";
    public static String YEAR = "year";
    public static String MONTH = "month";



}
