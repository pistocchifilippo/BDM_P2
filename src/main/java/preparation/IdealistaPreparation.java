package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class IdealistaPreparation implements Preparation <String,String>{

    private final Stream<Tuple2<String,String>> paths;

    public IdealistaPreparation(final Stream<Tuple2<String,String>> paths) {
        this.paths = paths;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        JavaRDD<Row> emptyRDD = new JavaSparkContext(spark.sparkContext()).emptyRDD();

//        JavaRDD<Row> rdds =
                paths
                .map(t -> new Tuple2<>(t._1, spark.read().parquet(t._2).javaRDD()))
                .map(t ->
                        t._2 // t._2 is a JavaRDD<Row> || not optimum
                )
                .reduce(emptyRDD, (a,b)->a.union(b))
                .foreach(e -> System.out.println(e));

        return null;

    }
}
