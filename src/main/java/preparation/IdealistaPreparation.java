package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.stream.Stream;


public class IdealistaPreparation implements Preparation <String,String>{

    private static final int PROPERTY_CODE_INDEX = 20;
    private static final String SEPARATOR = ",";

    private final Stream<Tuple2<String,String>> paths;

    public IdealistaPreparation(final Stream<Tuple2<String,String>> paths) {
        this.paths = paths;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        JavaRDD<String> emptyRDD = new JavaSparkContext(spark.sparkContext()).emptyRDD();

        return paths
                .map(t -> new Tuple2<>(t._1, spark.read().parquet(t._2).javaRDD()))
                .map(t ->
                        t._2.map(r -> t._1 + SEPARATOR + r.mkString(SEPARATOR))
                )
                .reduce(emptyRDD, (a,b) -> a.union(b))
                .mapToPair(l -> new Tuple2<>(l.split(SEPARATOR)[PROPERTY_CODE_INDEX],l));

    }
}
