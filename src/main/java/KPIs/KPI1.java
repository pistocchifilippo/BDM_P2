package KPIs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.IdealistaLutPreparation;
import preparation.IdealistaPreparation;
import scala.Tuple2;

public class KPI1 implements MyKPIs{

    public JavaRDD<String> retrieve(final SparkSession spark) {

        // Data preparation
        //==============================================================================================================
        // Idealista
        // (El Gòtic,2021_03_09,92331899,El Gòtic,160000.0)
        JavaPairRDD<String, String> idealista = new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);

        // Idealista Lookup table (neighborhood)
        // (El Gòtic,Q17154)
        JavaPairRDD<String, String> rent_lut = new IdealistaLutPreparation().prepare(spark);


        // Idealista by Neighborhood ID
        // (Q3596096,2020_12_07,89407269,Sants - Badal,120000.0)
        JavaPairRDD<String,String> idealistaByNeighID = rent_lut.join(idealista)
                                                        .mapToPair(t -> new Tuple2<>(t._2._1,t._2._2)).cache();

        /// KPI nº 1
        JavaRDD<String> query1 = idealistaByNeighID.mapToPair(s ->
                    new Tuple2<String, Integer>(s._2.split(",")[0].replace("_", "-"), 1))
                .reduceByKey((a,b) -> a + b).sortByKey().map(t -> (t._1+","+t._2));
       // query1.coalesce(1).saveAsTextFile(outputpath);
        return  query1;
    }
}
