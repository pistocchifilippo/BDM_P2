import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.*;
import scala.Tuple2;

public class Main {

	static final String APP_NAME = "BDM_P2";
	static final SparkSession spark = SparkSession
			.builder()
			.appName(APP_NAME)
			.master("local[*]")
			.config("spark.mongodb.input.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.config("spark.mongodb.output.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.getOrCreate();

	static final String INCOME_LUT = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
	static final String RENT_LUT = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";
	static final String AGE_DATASET = "src/main/resources/building_age/2020_edificacions_edat_mitjana.csv";
	static final String INCOME_DATASET = "src/main/resources/income_opendata/income_opendata_neighborhood.json";

	public static void main(String[] args) {

		final JavaPairRDD<String, String> oldness = new BuildingAgePreparation(AGE_DATASET).prepare(spark);

//		oldness.foreach(e -> System.out.println(e));

		// Idealista dataset
		final JavaPairRDD<String, String> idealista = new IdealistaPreparationNoDuplicates(IdealistaReader.allPairDateFilePath()).prepare(spark);
		idealista.foreach(e -> System.out.println(e));

		// Idealista Lookup table (neighborhood)
		JavaPairRDD<String, String> rent_lut = new IdealistaLutPreparation(RENT_LUT).prepare(spark);
		//rent_lut.foreach(e -> System.out.println(e));

		// Income Lookup table (neighborhood)
		JavaPairRDD<String, String> income_lut = new IncomeLutPreparation(INCOME_LUT).prepare(spark);
		//income_lut.foreach(e -> System.out.println(e));

		// Income Opendata dataset (neighborhood)
		JavaPairRDD<String, String> incomes = new IncomePreparation(INCOME_DATASET).prepare(spark);
		//incomes.foreach(s -> System.out.println(s));

		// Join: Income OpenData & LUT
		JavaPairRDD<String, Tuple2<String, String>> joinedIncome = income_lut.join(incomes);
		joinedIncome.foreach(s -> System.out.println(s));
		
		// Join: Idealista & LUT
		JavaPairRDD<String, Tuple2<String, String>> joinedIdealista = rent_lut.join(idealista);
		//joinedIdealista.foreach(s -> System.out.println(s));

		JavaPairRDD<String,String> idealistaByNeighID = joinedIdealista.mapToPair(t -> new Tuple2<>(t._2._1,t._2._2));
		//idealistaByNeighID.foreach(s -> System.out.println(s));

		JavaPairRDD<String,String> incomeByNeighID = joinedIncome.mapToPair(t -> new Tuple2<>(t._2._1,t._2._2));
		//incomeByNeighID.foreach(s -> System.out.println(s));

		JavaRDD<String> dataset = idealistaByNeighID.join(incomeByNeighID).map(t -> (t._1+","+t._2._1 +"," +t._2._2));
		dataset.foreach(s -> System.out.println(s));
		//	dataset
		//			.coalesce(1)
		//			.saveAsTextFile("data_result1.csv");


		JavaPairRDD<String, Integer > query1 = dataset
													.mapToPair(s -> new Tuple2<>(s.split(",")[1], 1))
													.reduceByKey((a,b) -> a + b);
		query1.foreach(s -> System.out.println(s));


	}

}

