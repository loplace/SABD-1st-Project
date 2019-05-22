package erreesse.resultsuploader.datarow;

import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Data
public class WrapperQuery2 extends AQueryDataRow{

    public String dataType;

    @Getter
    public List<Query2DataRow> dataBeans = new ArrayList<>();

    @Override
    public String getRowKey() {
        return null;
    }

    @Override
    public String getColumnValue() {
        return null;
    }
}