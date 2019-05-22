package erreesse.resultsuploader.client;

import erreesse.resultsuploader.datarow.AQueryDataRow;
import erreesse.resultsuploader.datarow.Query1DataRow;
import erreesse.resultsuploader.tabledescriptor.Query1TableDescriptor;
import erreesse.resultsuploader.tabledescriptor.Query2TableDescriptor;
import lombok.Getter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


public abstract class AGenericQueryBaseClient<T extends AQueryDataRow> extends HBaseClient {


    protected String tableName;
    @Getter
    protected List<T> results;

    public AGenericQueryBaseClient(String nameOfTable) {
        tableName = nameOfTable;
        results = new ArrayList<>();
    }

    protected static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    public static AGenericQueryBaseClient getClient(String tableName) {

        switch (tableName) {
            case "query1":
                return new Query1HBaseClient(Query1TableDescriptor.TABLE_NAME);
            case "query2":
                return new Query2HBaseClient(Query2TableDescriptor.TABLE_NAME);
            case "query3":
                return null;
            default:
                throw new IllegalArgumentException("Table not extists");
        }
    }

    public void clearOldResults() {
        if (exists(tableName)) {
            truncateTable(tableName,false);
        }
    }

    public abstract T parseLine(String line);

    public abstract void addDataRow(T parsedRowModel);

    public void putResults(String columnFamily) {

        for (T singleResult : results) {
            //hbc.put("products", "row1", "fam1", "col1", "val1");
            put(tableName,
                singleResult.getRowKey(),
                columnFamily,
                singleResult.getColumnName(),
                singleResult.getColumnValue());
        }
    }
}
