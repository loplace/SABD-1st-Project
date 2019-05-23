package queries.combiner;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorByTuple2Value implements Serializable, Comparator<Tuple2<String, Double>> {

    @Override
    public int compare(Tuple2<String, Double> t2, Tuple2<String, Double> t1) {
        return Double.compare((Double) t2._2(), (Double) t1._2());
    }
}