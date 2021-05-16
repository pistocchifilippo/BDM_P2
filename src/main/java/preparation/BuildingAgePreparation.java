package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.DecimalFormat;

public class BuildingAgePreparation implements Preparation <String,String> {

    private static final int NEIGH = 4;
    private static final int AGE = 6;

    private final String path;

    public BuildingAgePreparation(final String path) {
        this.path = path;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {
        return spark.read().csv(path).javaRDD()
                .mapToPair(e -> {
                    final String neigh = e.getString(NEIGH);
                    final double age = Double.parseDouble(e.getString(AGE));
                    return new Tuple2<>(neigh, new Tuple2<>(age, 1));
                })
                .reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapValues(e -> new DecimalFormat("0.00").format(e._1/e._2));
    }
}
