package parser.validators;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class HumidityValidator implements IMeasurementValidator {
    @Override
    public boolean isValid(String rawValue) {
        boolean notEmpty = rawValue!=null && !rawValue.isEmpty();
        boolean correctlyFormat = true;
        boolean outOfRange = false;

        double measuredValue = 0L;
        try{
            measuredValue = parseValue(rawValue);

            if (measuredValue>100L && measuredValue<0L) {
                outOfRange = true;
            }
        } catch (NumberFormatException e) {
            correctlyFormat = false;
        }
        return notEmpty && correctlyFormat && !outOfRange;
    }

    @Override
    public double adjustValue(double valueToAdjust) {
        return 0;
    }

    @Override
    public double parseValue(String rawValue) {
        return Double.parseDouble(rawValue);
    }
}
