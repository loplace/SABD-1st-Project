package parser.validators;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.Set;

public class TemperatureValidator {

    private static final char SEPARATOR = ',';



    public void preprocessTemperature(String path) throws IOException {

        Set<String> headers = null;
        FileReader reader = new FileReader(path);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).withIgnoreEmptyLines().parse(reader);
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");
        Iterator<CSVRecord> iterator = records.iterator();

        String pattern = "###.##";
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat(pattern,decimalFormatSymbols);




        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            for (String field : headers) {

                String measurement = record.get(field);
                double value = Double.parseDouble(measurement);
                int lenght = measurement.length();
                boolean dot = measurement.contains(".");

                if (!dot){

                    int exp = lenght - 3;
                    double divisor = Math.pow(10,exp);

                    value = value / divisor;

                }
                System.out.println("original measurement is: " + measurement);

                System.out.println("parsed measurement is: " + value);


            }
        }


      //  CSVWriter writer = new CSVWriter(new FileWriter(output),SEPARATOR,' ');
      //  writer.writeAll(csvBody);
      //  writer.flush();
      //  writer.close();

    }


}
