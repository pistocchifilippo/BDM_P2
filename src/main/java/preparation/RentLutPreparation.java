package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class RentLutPreparation implements Preparation <String,String> {

    private final String path;

    public RentLutPreparation(final String path) {
        this.path = path;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {
        return spark.read().json(path).javaRDD()
                .mapToPair(e -> new Tuple2<>(e.getString(0), e.getString(1)));
    }
}
