package utils.locationinfo;

import parser.CityAttributeParser;
import utils.locationinfo.LocationInfoAssigner;

import java.io.IOException;

public class CityAttributesPreprocessor {

    private CityAttributeParser cap = null;

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
