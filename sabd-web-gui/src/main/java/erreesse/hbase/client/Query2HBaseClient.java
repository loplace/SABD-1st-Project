package erreesse.hbase.client;

import com.google.protobuf.ServiceException;
import erreesse.bean.Query2ResultBean;
import erreesse.bean.Query3ResultBean;
import erreesse.hbase.tabledescriptor.Query2TableDescriptor;
import erreesse.hbase.tabledescriptor.Query3TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query2HBaseClient extends AGenericQueryBaseClient<Query2ResultBean> {

    public Query2HBaseClient() {
        super();
        tableName = Query2TableDescriptor.TABLE_NAME;
    }

    protected Query2ResultBean parseResult(Result result, String columnFamily) {
        byte[] rowkey = result.getRow();

        Query2ResultBean bean = null;

        // aId, uId, cId
        String rowKey = Bytes.toString(rowkey);


        String country = Bytes.toString(
                result.getValue(
                        b(Query2TableDescriptor.COUNTRY_INFO),
                        Bytes.toBytes(Query2TableDescriptor.COUNTRY)));

        String year = Bytes.toString(
                result.getValue(
                        b(Query2TableDescriptor.COUNTRY_INFO),
                        Bytes.toBytes(Query2TableDescriptor.YEAR)));

        String month = Bytes.toString(
                result.getValue(
                        b(Query2TableDescriptor.COUNTRY_INFO),
                        Bytes.toBytes(Query2TableDescriptor.MONTH)));

        String mean = Bytes.toString(
                result.getValue(
                        b(columnFamily),
                        Bytes.toBytes(Query2TableDescriptor.MEAN)));

        String devstd = Bytes.toString(
                result.getValue(
                        b(columnFamily),
                        Bytes.toBytes(Query2TableDescriptor.DEVSTD)));

        String min = Bytes.toString(
                result.getValue(
                        b(columnFamily),
                        Bytes.toBytes(Query2TableDescriptor.MIN)));

        String max = Bytes.toString(
                result.getValue(
                        b(columnFamily),
                        Bytes.toBytes(Query2TableDescriptor.MAX)));

        bean = new Query2ResultBean(country,year,month,mean,devstd,min,max);

        return bean;
    }

    @Override
    public List<Query2ResultBean> getResults(String columnFamily, String column) throws IOException, ServiceException {

        results = new ArrayList<>();

        Table products = getConnection().getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null) {
            scan.addFamily(b(columnFamily));
            scan.addFamily(b(Query2TableDescriptor.COUNTRY_INFO));
        }

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            System.out.println("columnFamily: "+columnFamily);
            System.out.println("Found row : " + result);
            results.add(parseResult(result,columnFamily));
        }

        scanner.close();

        return results;
    }
}
