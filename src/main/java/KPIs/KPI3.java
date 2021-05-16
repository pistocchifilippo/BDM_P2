package KPIs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.BuildingAgeLutPreparation;
import preparation.BuildingAgePreparation;
import preparation.IdealistaLutPreparation;
import preparation.IdealistaPreparation;
import scala.Tuple2;

import java.text.DecimalFormat;

public class KPI3 implements MyKPIs {

    static final String AGE_DATASET = "src/main/resources/building_age/2020_edificacions_edat_mitjana.csv";
    static final String AGE_LUT = "src/main/resources/building_age/buildingAge_lookup.csv";

    public JavaRDD<String> retrieve(final SparkSession spark) {

        // Data preparation
        //==============================================================================================================
        // Idealista
        // (El Gòtic,2021_03_09,92331899,El Gòtic,160000.0)
        JavaPairRDD<String, String> idealista = new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);
        //Idealista Lookup table (neighborhood)
        // (El Gòtic,Q17154)
        JavaPairRDD<String, String> rent_lut = new IdealistaLutPreparation().prepare(spark);


        // Join: Idealista & LUT
        // (Q3596096,2020_12_07,89407269,Sants - Badal,120000.0)
        JavaPairRDD<String,String> idealistaByNeighID = rent_lut.join(idealista)
                .mapToPair(t -> new Tuple2<>(t._2._1,t._2._2)).cache();

        //==============================================================================================================

        // Building Average age
        // (Ciutat Meridiana,50.42)
        // (Can Baró,59.94)
        JavaPairRDD<String, String> oldness = new BuildingAgePreparation(AGE_DATASET).prepare(spark);

        // Building Average age LUT
        // (Ciutat Meridiana,Q111111)
        JavaPairRDD<String, String> buildingAge_lut = new BuildingAgeLutPreparation(AGE_LUT).prepare(spark);

        // Join:Building Average age & LUT
        JavaPairRDD<String,String> buildingAgeByNeighID = buildingAge_lut.join(oldness).mapToPair(t -> new Tuple2<>(t._2._1, t._1 + "," + t._2._2));;

     //=================================================================================================================

        JavaRDD<String> kpi3 = idealistaByNeighID
                .mapToPair(s -> new Tuple2<>(s._1, new Tuple2<Double, Integer>(Double.parseDouble(s._2.split(",")[3]), 1)))
                .reduceByKey((a, b) -> {
                    Double res1 = a._1 + b._1;
                    int res2 = a._2 + b._2;
                    return new Tuple2<>(res1, res2);
                })
                .mapValues(s -> new DecimalFormat("0.00").format(s._1/s._2)) // (Q3294602,320000.00)
                .join(buildingAgeByNeighID).map(s -> s._1 + "," + s._2._1 + "," + s._2._2);

        return kpi3;


    }
}
