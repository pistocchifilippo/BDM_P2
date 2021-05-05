package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public interface Preparation <X,Y> extends Serializable {

    JavaPairRDD<X, Y> prepare(SparkSession spark);

    default String cleanString(final String s) {
        return s
                .replace("[","")
                .replace("]","")
                .replace("\"", "");
    }

}
