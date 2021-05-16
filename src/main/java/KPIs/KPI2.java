package KPIs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.IdealistaLutPreparation;
import preparation.IdealistaPreparation;
import preparation.IncomeLutPreparation;
import preparation.IncomePreparation;
import scala.Tuple2;

import java.text.DecimalFormat;

public class KPI2 implements MyKPIs {


    static String cleanString(final String s) {
        return s
                .replace("\"", "");
    }

    static final String INCOME_LUT = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
    static final String RENT_LUT = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";
    static final String INCOME_DATASET = "src/main/resources/income_opendata/income_opendata_neighborhood.json";


    public JavaRDD<String> retrieve(final SparkSession spark) {

        // Data preparation
        //==============================================================================================================
        // Idealista
        // (El Gòtic,2021_03_09,92331899,El Gòtic,160000.0)
        JavaPairRDD<String, String> idealista = new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);
        //Idealista Lookup table (neighborhood)
        // (El Gòtic,Q17154)
        JavaPairRDD<String, String> rent_lut = new IdealistaLutPreparation(RENT_LUT).prepare(spark);


        // Join: Idealista & LUT
        // (Q3596096,2020_12_07,89407269,Sants - Badal,120000.0)
        JavaPairRDD<String,String> idealistaByNeighID = rent_lut.join(idealista)
                                                        .mapToPair(t -> new Tuple2<>(t._2._1,t._2._2)).cache();

       //==============================================================================================================

        // Income Lookup table (neighborhood)
        // (el Poblenou,Q1404773,Reconciled)
        JavaPairRDD<String, String> income_lut = new IncomeLutPreparation(INCOME_LUT).prepare(spark);

        // Income Opendata dataset (neighborhood)
        // (el Poble Sec,2017,40358,82.2)
        JavaPairRDD<String, String> incomes = new IncomePreparation(INCOME_DATASET).prepare(spark);

        // Join: Income OpenData & LUT
        JavaPairRDD<String, String> incomeByNeighID = income_lut.join(incomes)
                                                            .mapToPair(t -> new Tuple2<>(t._2._1.split(",")[0],cleanString(t._2._1.split(",")[1])
                                                                    +","+t._2._2.split(",")[2])).cache();

        //==============================================================================================================


        JavaRDD<String> kpi2 = idealistaByNeighID
                .mapToPair(s -> new Tuple2<>(s._1, new Tuple2<Double, Integer>(Double.parseDouble(s._2.split(",")[3]), 1)))
                .reduceByKey((a, b) -> {
                    Double res1 = a._1 + b._1;
                    int res2 = a._2 + b._2;
                    return new Tuple2<>(res1, res2);
                })
                .mapValues(s -> new DecimalFormat("0.00").format(s._1/s._2)) // (Q3294602,320000.00)
                .join(incomeByNeighID).map(s -> s._1 + "," + s._2._1 + "," + s._2._2);


        return kpi2;

    }

}