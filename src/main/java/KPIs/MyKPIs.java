package KPIs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public interface MyKPIs<X> extends Serializable {

    JavaRDD<String> retrieve(SparkSession spark);


}
