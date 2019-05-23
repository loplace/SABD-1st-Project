package queries.combiner;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;

public abstract class ARankCombiner implements Serializable {

    static ComparatorByTuple2Value myComparator = new ComparatorByTuple2Value();

    public abstract Function<Tuple2<String, Double>, List<Tuple2<String, Double>>> createAccumulator();

    public abstract Function2<List<Tuple2<String, Double>>, Tuple2<String, Double>, List<Tuple2<String, Double>>> createMergeValue();

    public abstract Function2<List<Tuple2<String, Double>>, List<Tuple2<String, Double>>, List<Tuple2<String, Double>>> createMergeCombiner();

    protected abstract List<Tuple2<String, Double>> truncate(List<Tuple2<String, Double>> v1);
}
