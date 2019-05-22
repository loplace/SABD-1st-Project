package erreesse.resultsuploader.datarow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;


@Data
@AllArgsConstructor
public class Query2DataRow  extends AQueryDataRow {

    @Getter
    private String country;
    @Getter
    private String year;
    @Getter
    private String month;

    private String[] values;

    @Override
    public String getRowKey() {
        return country+"_"+year+"_"+month;
    }

    @Override
    public String getColumnValue() {
        return null;
    }

    public String getColumnValueAtIndex(int i) {
        return values[i];
    }
}
