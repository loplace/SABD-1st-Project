package erreesse.hbase.client;

import com.google.protobuf.ServiceException;
import erreesse.bean.Query1ResultBean;
import erreesse.hbase.tabledescriptor.Query1TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AGenericQueryBaseClient<T> extends HBaseClient {

    protected List<T> results;
    protected String tableName;

    public AGenericQueryBaseClient() {
        results = new ArrayList<>();
    }

    protected static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    protected abstract T parseResult(Result result, String columnFamily);

    public List<T> getResults(String columnFamily, String column) throws IOException, ServiceException {

        Table products = getConnection().getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null)
            scan.addFamily(b(columnFamily));

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            //System.out.println("Found row : " + result);
            results.add(parseResult(result,columnFamily));
        }

        scanner.close();

        return results;
    }

}
