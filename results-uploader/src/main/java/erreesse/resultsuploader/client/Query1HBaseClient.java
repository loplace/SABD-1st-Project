package erreesse.resultsuploader.client;


import erreesse.resultsuploader.datarow.Query1DataRow;

public class Query1HBaseClient extends AGenericQueryBaseClient<Query1DataRow> {

    public Query1HBaseClient(String tname) {
        super(tname);
    }

    @Override
    public Query1DataRow parseLine(String line) {
        /*
        2014,[Los Angeles, Jerusalem, Las Vegas]
        2016,[Eilat, Phoenix, Las Vegas]
        2015,[Jerusalem, Eilat]
        2013,[Las Vegas, Eilat]
        2017,[Phoenix, Eilat, Las Vegas]
        */
        String[] tokens = line.split(",",2);
        return new Query1DataRow(tokens[0],tokens[1]);
    }

    @Override
    public void addDataRow(Query1DataRow parsedRowModel) {
        results.add(parsedRowModel);
    }
}
