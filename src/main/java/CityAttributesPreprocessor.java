import net.xdevelop.jpclient.PyServeException;
import utils.CityAttributeParser;
import utils.TimezoneAssigner;

import java.io.IOException;

public class CityAttributesPreprocessor {

    private CityAttributeParser cap = null;

/*    public static void main(String[] args) {
        CityAttributeParser cap = new CityAttributeParser();
        try {
            cap.parse();
            TimezoneAssigner.assignTimezone(cap.getCities());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (PyServeException e) {
            e.printStackTrace();
        }

        cap.printCities();
    }*/

    public CityAttributeParser process(){

        this.cap = new CityAttributeParser();
        try {
            cap.parse();
            TimezoneAssigner.assignTimezone(cap.getCities());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (PyServeException e) {
            e.printStackTrace();
        }

        return this.cap;
    }

}
