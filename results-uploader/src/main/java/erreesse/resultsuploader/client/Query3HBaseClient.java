package erreesse.resultsuploader.client;

import erreesse.resultsuploader.datarow.Query3DataRow;
import erreesse.resultsuploader.tabledescriptor.Query3TableDescriptor;

public class Query3HBaseClient extends AGenericQueryBaseClient<Query3DataRow> {

    public Query3HBaseClient(String tname) {
        super(tname);
    }

    @Override
    public Query3DataRow parseLine(String line) {
        /*
        Israel,Beersheba,12.807026594827732,1,14.299389615280063,1
        USA,Detroit,21.549851542229476,1,18.465875531273866,2
        Israel,Eilat,9.865982669080779,3,14.258489062764511,2
        Israel,Tel Aviv District,10.808761202558912,2,10.935187992395868,4
        USA,Pittsburgh,19.08617872558574,3,15.504326459860238,10
        USA,Chicago,20.703123498225693,2,18.62785421105741,1
        */
        String[] tokens = line.split(",");
        return new Query3DataRow(
                tokens[0],
                tokens[1],
                tokens[2],
                tokens[3],
                tokens[4],
                tokens[5]
                );
    }

    @Override
    public void addDataRow(Query3DataRow parsedRowModel) {
        results.add(parsedRowModel);
    }

    @Override
    public void putResults(String columnFamily) {

        for (Query3DataRow singleResult : results) {
            uploadCountryCity(singleResult);
            uploadData2017(singleResult);
            uploadData2016(singleResult);
        }
    }

    private void uploadCountryCity(Query3DataRow result) {
        boolean put = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.CITY_INFO_COLUMN_FAMILY,
                Query3TableDescriptor.COUNTRY,
                result.getCountry());
        if (put) uploadedCounter++;

        boolean put1 = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.CITY_INFO_COLUMN_FAMILY,
                Query3TableDescriptor.CITY,
                result.getCity());
        if (put1) uploadedCounter++;
    }

    private void uploadData2017(Query3DataRow result) {
        boolean put = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.DATES_2017_COLUMN_FAMILY,
                Query3TableDescriptor.ABS_MEAN_DIFF,
                result.getAbsMean2017());
        if (put) uploadedCounter++;

        boolean put1 = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.DATES_2017_COLUMN_FAMILY,
                Query3TableDescriptor.POS,
                result.getPos2017());
        if (put1) uploadedCounter++;
    }

    private void uploadData2016(Query3DataRow result) {
        boolean put = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.DATES_2016_COLUMN_FAMILY,
                Query3TableDescriptor.ABS_MEAN_DIFF,
                result.getAbsMean2016());
        if (put) uploadedCounter++;

        boolean put1 = put(tableName,
                result.getRowKey(),
                Query3TableDescriptor.DATES_2016_COLUMN_FAMILY,
                Query3TableDescriptor.POS,
                result.getPos2016());
        if (put1) uploadedCounter++;
    }
}
