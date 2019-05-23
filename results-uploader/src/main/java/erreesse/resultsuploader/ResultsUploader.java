package erreesse.resultsuploader;

import erreesse.resultsuploader.client.AGenericQueryBaseClient;
import erreesse.resultsuploader.client.HBaseClient;

import java.util.Scanner;

public class ResultsUploader {

    private static AGenericQueryBaseClient hBaseClient;

    public static void main(String[] args) {

        String tableName = args[0];
        String columnFamily = args[1];

        boolean truncateTable = Boolean.parseBoolean(args[2]);

        hBaseClient = AGenericQueryBaseClient.getClient(tableName);
        if(truncateTable) {
            hBaseClient.clearOldResults();
        }

        System.out.println("Started Results Uploader with args:");
        System.out.println("tableName: "+tableName);
        System.out.println("columnFamily: "+columnFamily);

        // connect to stdin
        System.out.println("Start parsing results");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            if (!line.isEmpty()) {
                hBaseClient.addDataRow(hBaseClient.parseLine(line));
            }
        }
        System.out.println("End parsing results");
        System.out.println("Total results to upload: "+hBaseClient.getResults().size());

        System.out.println("Start uploading results");
        hBaseClient.putResults(columnFamily);
        System.out.println("End uploading results");
        System.out.println("Uploaded "+ hBaseClient.getUploadedCounter()+" results");
    }
}
