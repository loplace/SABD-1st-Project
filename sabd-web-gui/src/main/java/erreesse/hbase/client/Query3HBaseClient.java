package erreesse.hbase.client;

import erreesse.bean.Query3ResultBean;
import erreesse.hbase.tabledescriptor.Query3TableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Query3HBaseClient extends AGenericQueryBaseClient<Query3ResultBean> {

    public Query3HBaseClient() {
        super();
        tableName = Query3TableDescriptor.TABLE_NAME;
    }

    protected Query3ResultBean parseResult(Result result, String columnFamily) {
        byte[] rowkey = result.getRow();

        Query3ResultBean bean = null;

        // aId, uId, cId
        String city = Bytes.toString(rowkey);


        String absMean2017 = Bytes.toString(
                result.getValue(
                        b(Query3TableDescriptor.DATES_2017_COLUMN_FAMILY),
                        Bytes.toBytes(Query3TableDescriptor.ABS_MEAN_DIFF)));

        String pos2017 = Bytes.toString(
                result.getValue(
                        b(Query3TableDescriptor.DATES_2017_COLUMN_FAMILY),
                        Bytes.toBytes(Query3TableDescriptor.POS)));

        String absMean2016 = Bytes.toString(
                result.getValue(
                        b(Query3TableDescriptor.DATES_2016_COLUMN_FAMILY),
                        Bytes.toBytes(Query3TableDescriptor.ABS_MEAN_DIFF)));

        String pos2016 = Bytes.toString(
                result.getValue(
                        b(Query3TableDescriptor.DATES_2016_COLUMN_FAMILY),
                        Bytes.toBytes(Query3TableDescriptor.POS)));

        bean = new Query3ResultBean(city,absMean2017,pos2017,absMean2016,pos2016);

        return bean;
    }
}
