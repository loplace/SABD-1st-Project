package parser.validators;

import java.io.Serializable;

public interface IMeasurementValidator extends Serializable {

    boolean isValid(String rawValue);

    double adjustValue(double valueToAdjust);

    double parseValue(String rawValue);
}
