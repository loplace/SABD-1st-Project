package queries.combiner;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TruncatedCombiner extends CompleteCombiner {

    @Override
    protected List<Tuple2<String, Double>> truncate(List<Tuple2<String, Double>> v1) {
        if (v1.size() >3 ) {
            return new ArrayList<>(v1.subList(0,3));
        } else return v1;
    }
}
