package erreesse.resultsuploader.client;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class AGenericQueryBaseClient<T> extends HBaseClient {


    protected String tableName;

    public AGenericQueryBaseClient() {

    }

    protected static byte[] b(String s){
        return Bytes.toBytes(s);
    }



}
