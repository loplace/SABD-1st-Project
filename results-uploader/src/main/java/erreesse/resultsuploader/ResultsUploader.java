package erreesse.resultsuploader;

import erreesse.resultsuploader.client.HBaseClient;
import erreesse.resultsuploader.tabledescriptor.Query1TableDescriptor;
import erreesse.resultsuploader.tabledescriptor.Query2TableDescriptor;
import erreesse.resultsuploader.tabledescriptor.Query3TableDescriptor;

public class ResultsUploader {

    private static HBaseClient hBaseClient;

    private static void clearOldResults() {
        if (hBaseClient.exists(Query1TableDescriptor.TABLE_NAME)) {
            hBaseClient.dropTable(Query1TableDescriptor.TABLE_NAME);
        }
        if (hBaseClient.exists(Query2TableDescriptor.TABLE_NAME)) {
            hBaseClient.dropTable(Query2TableDescriptor.TABLE_NAME);
        }
        if (hBaseClient.exists(Query3TableDescriptor.TABLE_NAME)) {
            hBaseClient.dropTable(Query3TableDescriptor.TABLE_NAME);
        }
    }


    public static void main(String[] args) {

        hBaseClient = new HBaseClient();
        clearOldResults();




    }
}
