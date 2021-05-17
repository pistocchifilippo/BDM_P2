package KPIs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.IdealistaLutPreparation;
import preparation.IdealistaPreparation;
import scala.Tuple2;

public class KPI1 implements MyKPIs{

    private static final int DATE = 0;

    public JavaRDD<String> retrieve(final SparkSession spark) {

        // Data preparation
        //==============================================================================================================
        // Idealista
        // (El Gòtic,2021_03_09,92331899,El Gòtic,160000.0)
        JavaPairRDD<String, String> idealista = new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);

        /// KPI nº 1
        JavaRDD<String> query1 = idealista
                .mapToPair(s -> new Tuple2<String, Integer>(s._2.split(";")[DATE].replace("_", "-"), 1))
                .reduceByKey((a,b) -> a + b)
                .sortByKey()
                .map(t -> (t._1+";"+t._2)); // to print the result without parenthesis

        return  query1;
    }
}
