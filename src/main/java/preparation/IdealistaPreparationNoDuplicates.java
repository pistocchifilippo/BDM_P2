package preparation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.stream.Stream;

public class IdealistaPreparationNoDuplicates implements Preparation<String, String> {

    private static final int NEIGHBOURHOOD = 20;
    private static final int PROPERTY_ID = 28;
    private static final String SEPARATOR = ",";

    private final Stream<Tuple2<String,String>> paths;

    public IdealistaPreparationNoDuplicates(final Stream<Tuple2<String,String>> paths) {
        this.paths = paths;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        final JavaRDD<String> emptyRDD = new JavaSparkContext(spark.sparkContext()).emptyRDD();

        return paths
                .map(t -> new Tuple2<>(t._1, spark.read().parquet(t._2).javaRDD()))
                .map(t ->
                        t._2.map(r -> t._1 + SEPARATOR + r.mkString(SEPARATOR))
                )
                .reduce(emptyRDD, (a,b) -> a.union(b))
                .filter(e -> !e.split(SEPARATOR)[NEIGHBOURHOOD].equals("null") && !e.equals("true") && !e.equals("false"))
                .mapToPair(l -> new Tuple2<>(l.split(SEPARATOR)[PROPERTY_ID], l))
                .reduceByKey((String a, String b) -> (a.split(SEPARATOR)[0].compareTo(b.split(SEPARATOR)[0])) < 0 ? a : b)
                .mapToPair(l -> new Tuple2<>(l._2.split(SEPARATOR)[NEIGHBOURHOOD], l._2));
    }

}