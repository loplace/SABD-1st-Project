package erreesse.resultsuploader.datarow;

import erreesse.resultsuploader.tabledescriptor.Query1TableDescriptor;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class Query1DataRow extends AQueryDataRow {

    private String year;

    private String citiesList;

    public Query1DataRow(String year, String citiesList) {
        this.year = year;
        this.citiesList = citiesList;
        columnName = Query1TableDescriptor.LIST;
    }

    @Override
    public String getRowKey() {
        return year;
    }

    @Override
    public String getColumnValue() {
        return citiesList;
    }
}
