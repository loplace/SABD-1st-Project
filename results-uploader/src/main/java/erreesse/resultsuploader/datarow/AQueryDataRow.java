package erreesse.resultsuploader.datarow;

import lombok.Getter;

public abstract class AQueryDataRow {
    @Getter
    protected String columnName;

    public abstract String getRowKey();

    public abstract String getColumnValue();
}
