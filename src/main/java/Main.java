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

	//static final SparkSession spark = SparkSession
	//		.builder()
	//		.appName(APP_NAME)
	//		.master("local[*]")
	//		.getOrCreate();

	static final String INCOME_LUT = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
	static final String RENT_LUT = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";
	static final String AGE_DATASET = "src/main/resources/building_age/2020_edificacions_edat_mitjana.csv";
	static final String INCOME_DATASET = "src/main/resources/income_opendata/income_opendata_neighborhood.json";

	static final SparkSession SparkMongoDB_income = SparkSession.builder()
			.master("local")
			.appName("income_lut")
			.config("spark.mongodb.input.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.config("spark.mongodb.output.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.getOrCreate();

	public static void main(String[] args) throws Exception {


		// Data preparation

		// (la Nova Esquerra de l'Eixample,Q1026658)
		JavaPairRDD<String, String> income_lut = new IncomeLutPreparation(INCOME_LUT).prepare(SparkMongoDB_income);
		income_lut.foreach(e -> System.out.println(e));


		//
		JavaPairRDD<String, String> incomes = new IncomePreparation(INCOME_DATASET).prepare(SparkMongoDB_income);
		incomes.foreach(s -> System.out.println(s));


		JavaPairRDD<String, Tuple2<String, String>> joinedRdd = income_lut.join(incomes);
		joinedRdd.foreach(s -> System.out.println(s));



		
		SparkMongoDB_income.close();



	}


}

