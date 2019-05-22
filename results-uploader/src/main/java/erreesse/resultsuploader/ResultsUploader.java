package erreesse.resultsuploader;

import erreesse.resultsuploader.client.AGenericQueryBaseClient;

import java.util.Scanner;

public class ResultsUploader {

    private static AGenericQueryBaseClient hBaseClient;




    public static void main(String[] args) {

        String tableName = args[0];
        String columnFamily = args[1];

        hBaseClient = AGenericQueryBaseClient.getClient(tableName);
        hBaseClient.clearOldResults();

        // connect to stdin
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            hBaseClient.addDataRow(hBaseClient.parseLine(line));
        }

        hBaseClient.putResults(columnFamily);


    }
}
