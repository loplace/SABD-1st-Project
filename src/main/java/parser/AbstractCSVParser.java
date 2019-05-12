package parser;
import model.CityKey;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractCSVParser<T extends CityKey> {

    private Map<String,T> mapOfModels;
    private Reader reader;
    private String filePath;
    private Iterable<CSVRecord> records;


    public AbstractCSVParser(String filePath) throws FileNotFoundException {

        this.filePath = filePath;
        reader = new FileReader(filePath);

        this.mapOfModels = new HashMap<>();

    }

    public void startParsing() throws IOException {

        records= CSVFormat.DEFAULT.withHeader().parse(reader);

        Iterator<CSVRecord> iterator = records.iterator();

        while (iterator.hasNext()){

            CSVRecord record = iterator.next();
            T model = parseRecord(record);

            String city = model.getCity();
            mapOfModels.put(city,model);

        }
    }

    protected abstract T parseRecord(CSVRecord record);

    public void printParsedData(){

        for(Map.Entry<String, T> entry: this.mapOfModels.entrySet()){

            System.out.println(entry.getKey() + "/" + entry.getValue());
        }

    }



}
