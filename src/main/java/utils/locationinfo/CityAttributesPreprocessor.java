package utils.locationinfo;

import parser.CityAttributeParser;
import utils.locationinfo.LocationInfoAssigner;

import java.io.IOException;

public class CityAttributesPreprocessor {

    private CityAttributeParser cap = null;

    public CityAttributeParser process(){

        this.cap = new CityAttributeParser();
        cap.parse();
        LocationInfoAssigner.locationInfoAssign(cap.getCities());

        return this.cap;
    }

}
