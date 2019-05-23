package erreesse.resultsuploader.datarow;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Query3DataRow  extends AQueryDataRow {

    private String country;

    private String city;

    private String absMean2017;

    private String pos2017;

    private String absMean2016;

    private String pos2016;

    @Override
    public String getRowKey() {
        return country+"_"+pos2017;
    }

    @Override
    public String getColumnValue() {
        return null;
    }
}
