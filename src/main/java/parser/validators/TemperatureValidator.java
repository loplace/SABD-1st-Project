package parser.validators;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class TemperatureValidator  implements IMeasurementValidator{

    private static double thresholdTemp = 400L;

    @Override
    public boolean isValid(String rawValue) {
        boolean notEmpty = rawValue!=null && !rawValue.isEmpty();
        boolean correctlyFormat = true;

        boolean outOfRange = false;
        double valueMeasured =0L;
        try{
            valueMeasured = parseValue(rawValue);
            if (valueMeasured> thresholdTemp) {
                outOfRange = true;
            }
        } catch (NumberFormatException e) {
            correctlyFormat = false;
        }
        return notEmpty && correctlyFormat && !outOfRange;
    }

    @Override
    public double parseValue(String stringValue) {
        double rawValue = Double.parseDouble(stringValue);
        return adjustValue(rawValue);
    }

    @Override
    public double adjustValue(double valueToAdjust) {

        // validazione temperatura in kelvin
        // se valueToAdjust > 400K (126,85°)
        double result = valueToAdjust;
        if (valueToAdjust >= thresholdTemp) {
            String text = Double.toString(valueToAdjust);
            int integerPlaces = text.indexOf('.');
            //int decimalPlaces = text.length() - integerPlaces - 1;

            if (integerPlaces>=4) {
                double coeff = Math.pow(10L,integerPlaces-3);
                valueToAdjust = valueToAdjust / coeff;
            }
        }

        return valueToAdjust;
    }

    public static void main(String[] args) {

        String test1String = "";

        TemperatureValidator tval = new TemperatureValidator();

        boolean valid1 = tval.isValid(test1String);
        double d1;

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");







        test1String = "pippo";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");



        test1String = "234.234";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");


        test1String = "296523";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");


        test1String = "450";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");



    }


}
