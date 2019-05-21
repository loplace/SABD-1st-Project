package erreesse.resultsuploader.resultparser;

import lombok.Getter;
import lombok.NoArgsConstructor;
import resultsuploader.datarow.Query1DataRow;
import resultsuploader.datarow.Query3DataRow;
import resultsuploader.datarow.WrapperQuery2;

import java.util.List;

@NoArgsConstructor
public class ResultParser {

    @Getter
    private List<Query1DataRow> query1DataToInsert;
    @Getter
    private List<WrapperQuery2> query2DataToInsert;
    @Getter
    private List<Query3DataRow> query3DataToInsert;


    public void parse() {
        parseDataQuery1();
        parseDataQuery2();
        parseDataQuery3();
    }

    private void parseDataQuery3() {

    }

    private void parseDataQuery2() {

    }

    private void parseDataQuery1() {


    }



}
