package erreesse.hbase.client;

import erreesse.bean.Query1ResultBean;
import erreesse.hbase.tabledescriptor.Query1TableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Query1HBaseClient extends AGenericQueryBaseClient<Query1ResultBean> {

    public Query1HBaseClient() {
        super();
        tableName = Query1TableDescriptor.TABLE_NAME;
    }

    protected Query1ResultBean parseResult(Result result, String columnFamily) {
        byte[] rowkey = result.getRow();

        Query1ResultBean bean = null;

        // aId, uId, cId
        String orderKey = Bytes.toString(rowkey);
        String citiesList = Bytes.toString(result.getValue(b(Query1TableDescriptor.COLUMN_FAMILY), Bytes.toBytes("list")));

        bean = new Query1ResultBean(orderKey,citiesList);
        return bean;
    }
}
