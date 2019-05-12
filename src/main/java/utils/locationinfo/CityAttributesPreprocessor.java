package utils.locationinfo;

import parser.CityAttributeParser;
import utils.locationinfo.LocationInfoAssigner;

import java.io.IOException;

public class CityAttributesPreprocessor {

    private CityAttributeParser cap = null;

/*    public static void main(String[] args) {
        CityAttributeParser cap = new CityAttributeParser();
        try {
            cap.parse();
            LocationInfoAssigner.locationInfoAssign(cap.getCities());

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
            LocationInfoAssigner.locationInfoAssign(cap.getCities());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return this.cap;
    }

}
