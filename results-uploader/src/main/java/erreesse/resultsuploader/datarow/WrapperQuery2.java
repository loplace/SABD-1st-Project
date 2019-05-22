package erreesse.resultsuploader.datarow;

import lombok.Data;

import java.util.List;

@Data
public class WrapperQuery2 {

    public String dataType;

    public List<Query2DataRow> dataBeans;
}