package resultsuploader.datarow;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Query2DataRow {

    private String country;
    private String year;
    private String month;

    private String mean;
    private String devStd;
    private String min;
    private String max;
}
