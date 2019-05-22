package erreesse.resultsuploader.datarow;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Query2DataRow  extends AQueryDataRow {

    private String country;
    private String year;
    private String month;

    private String mean;
    private String devStd;
    private String min;
    private String max;

    @Override
    public String getRowKey() {
        return country+"_"+year+"_"+month;
    }

    @Override
    public String getColumnValue() {
        return null;
    }
}
