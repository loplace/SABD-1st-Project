package queries.combiner;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompleteCombiner extends ARankCombiner {

    static ComparatorByTuple2Value myComparator = new ComparatorByTuple2Value();

    public  Function<Tuple2<String, Double>, List<Tuple2<String, Double>>> createAccumulator() {
        Function<Tuple2<String, Double>, List<Tuple2<String, Double>>> createAcc = new Function<Tuple2<String, Double>, List<Tuple2<String, Double>>>() {
            @Override
            public List<Tuple2<String, Double>> call(Tuple2<String, Double> x) {
                return new ArrayList<>();
            }
        };

        return createAcc;
    }

    public Function2<List<Tuple2<String, Double>>, Tuple2<String, Double>, List<Tuple2<String, Double>>> createMergeValue() {
        Function2<List<Tuple2<String, Double>>, Tuple2<String, Double>, List<Tuple2<String, Double>>> mergeValue = new Function2<List<Tuple2<String, Double>>, Tuple2<String, Double>, List<Tuple2<String, Double>>>() {
            @Override
            public List<Tuple2<String, Double>> call(List<Tuple2<String, Double>> v1, Tuple2<String, Double> v2) throws Exception {
                v1.add(v2);
                Collections.sort(v1,myComparator.reversed());
                v1 = truncate(v1);
                return v1;
            }
        };
        return mergeValue;

    }

    public Function2<List<Tuple2<String, Double>>, List<Tuple2<String, Double>>, List<Tuple2<String, Double>>> createMergeCombiner() {

        Function2<List<Tuple2<String, Double>>,List<Tuple2<String, Double>>,List<Tuple2<String, Double>>> mergeCombiners = new Function2<List<Tuple2<String, Double>>, List<Tuple2<String, Double>>, List<Tuple2<String, Double>>>() {
            @Override
            public List<Tuple2<String, Double>> call(List<Tuple2<String, Double>> v1, List<Tuple2<String, Double>> v2) throws Exception {
                v1.addAll(v2);
                Collections.sort(v1,myComparator.reversed());
                v1 = truncate(v1);
                return v1;
            }
        };

        return mergeCombiners;
    }

    @Override
    protected List<Tuple2<String, Double>> truncate(List<Tuple2<String, Double>> v1) {
        return v1;
    }
}
