package parser.validators;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class PressureValidator implements IMeasurementValidator{
    @Override
    public boolean isValid(String rawValue) {
        boolean notEmpty = rawValue!=null && !rawValue.isEmpty();
        boolean correctlyFormat = true;
        try{
            parseValue(rawValue);
        } catch (NumberFormatException e) {
            correctlyFormat = false;
        }
        return notEmpty && correctlyFormat;
    }

    @Override
    public double adjustValue(double valueToAdjust) {
        return 0;
    }

    @Override
    public double parseValue(String rawValue) {
        return Double.parseDouble(rawValue);
    }


    public static void main(String[] args) {


        String test1String = "";

        PressureValidator tval = new PressureValidator();

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




        test1String = "13.0";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");



        test1String = "1021.00";
        valid1 = tval.isValid(test1String);

        System.out.print(test1String + ", valid: "+valid1);
        if (valid1) {
            d1 = tval.parseValue(test1String);
            System.out.print(test1String + ", parsed: "+d1);
        }
        System.out.println("");

    }
}
