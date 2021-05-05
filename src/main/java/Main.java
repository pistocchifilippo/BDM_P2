import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import preparation.*;
import scala.Tuple2;

public class Main {

	static final String APP_NAME = "BDM_P2";
	static final String PARQUET_FILE = "src/main/resources/idealista/2020_01_02_idealista/part-00000-73a2bf77-0d24-4f05-bf18-7351de8d7938-c000.snappy.parquet";
	static final SparkSession spark = SparkSession
			.builder()
			.appName(APP_NAME)
			.master("local[*]")
			.getOrCreate();

	static final String INCOME_LUT = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
	static final String RENT_LUT = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";
	static final String AGE_DATASET = "src/main/resources/building_age/2020_edificacions_edat_mitjana.csv";
	static final String INCOME_DATASET = "src/main/resources/income_opendata/income_opendata_neighborhood.json";

	public static void main(String[] args) throws Exception {

		// Data preparation

		// (Sant Genís dels Agudells,51.54)
		JavaPairRDD<String, String> oldness = new BuildingAgePreparation(AGE_DATASET).prepare(spark);

//		oldness.foreach(e -> System.out.println(e));

		// (la Nova Esquerra de l'Eixample,Q1026658)
		JavaPairRDD<String, String> income_lut = new IncomeLutPreparation(INCOME_LUT).prepare(spark);

//		income_lut.foreach(e -> System.out.println(e));


		// ....
		JavaPairRDD<String, String> incomes = null;


		// (Q1026658,La Nova Esquerra de l'Eixample)
		JavaPairRDD<String, String> rent_lut = new RentLutPreparation(RENT_LUT).prepare(spark);

//		rent_lut.foreach(e -> System.out.println(e));

		new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);






		// Join(s)


		// (Sant Genís dels Agudells,(Q3298510,51.54))
		JavaPairRDD<String,Tuple2<String,String>> income_lut_2 = income_lut.join(oldness);

//		income_lut_2.foreach(e -> System.out.println(e));

//		income_lut_2
//				.mapToPair(e -> new Tuple2<>(e._2._1,new Tuple2<>(e._1,e._2._2)))
//				.join(rent_lut)
//				.mapToPair(e -> new Tuple2<>(e._2._1._1,e._2._2));



//		new IncomePreparation().prepare(INCOME_DATASET,spark);




	}


}

