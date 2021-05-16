package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.DecimalFormat;

public class BuildingAgeLutPreparation implements Preparation <String,String> {

    private final String path;

    public BuildingAgeLutPreparation(final String path) {
        this.path = path;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {
        return spark.read().csv(path).javaRDD()
                .mapToPair(e -> {
                    final String neigh = e.getString(0);
                    final String id = e.getString(1);
                    return new Tuple2<>(neigh, id);
                });
    }
}
