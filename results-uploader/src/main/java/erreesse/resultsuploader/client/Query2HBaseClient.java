package erreesse.resultsuploader.client;



import erreesse.resultsuploader.datarow.Query2DataRow;
import erreesse.resultsuploader.datarow.WrapperQuery2;
import erreesse.resultsuploader.tabledescriptor.Query2TableDescriptor;

public class Query2HBaseClient extends AGenericQueryBaseClient<WrapperQuery2> {

    private String[] columnNames = {Query2TableDescriptor.MEAN,
            Query2TableDescriptor.DEVSTD,
            Query2TableDescriptor.MIN,
            Query2TableDescriptor.MAX};


    public Query2HBaseClient(String tname) {
        super(tname);
    }


    @Override
    public WrapperQuery2 parseLine(String line) {
        /*
        Israel,2012,10,57.65837479270316,16.487245876180573,12.0,100.0
        */
        String[] tokens = line.split(",");
        String[] datas = {tokens[3],tokens[4],tokens[5],tokens[6]};
        Query2DataRow query2DataRow = new Query2DataRow(tokens[0], tokens[1], tokens[2], datas);
        WrapperQuery2 wq2 = new WrapperQuery2(); wq2.dataBeans.add(query2DataRow);
        return wq2;
    }

    @Override
    public void addDataRow(WrapperQuery2 parsedRowModel) {
        results.add(parsedRowModel);
    }

    @Override
    public void putResults(String columnFamily) {
        for (WrapperQuery2 singleResult : results) {
            //hbc.put("products", "row1", "fam1", "col1", "val1");
            Query2DataRow query2DataRow = singleResult.getDataBeans().get(0);
            if (columnFamily.equals("country_info")) {
                insertAllCountryInfosValues(Query2TableDescriptor.COUNTRY_INFO,query2DataRow);
            }else {
                insertAllMeasurementValues(columnFamily,query2DataRow);
            }

        }
    }

    private void insertAllMeasurementValues(String columnFamily, Query2DataRow query2DataRow) {

        for (int i=0; i<4; i++) {
            put(tableName,
                    query2DataRow.getRowKey(),
                    columnFamily,
                    columnNames[i],
                    query2DataRow.getColumnValueAtIndex(i));
        }

    }

    private void insertAllCountryInfosValues(String columnFamily, Query2DataRow q2) {
        put(tableName,q2.getRowKey(),columnFamily, Query2TableDescriptor.COUNTRY,q2.getCountry());
        put(tableName,q2.getRowKey(),columnFamily, Query2TableDescriptor.YEAR,q2.getYear());
        put(tableName,q2.getRowKey(),columnFamily, Query2TableDescriptor.MONTH,q2.getMonth());

    }

}
