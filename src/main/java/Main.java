import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import preparation.*;
import scala.Tuple2;

import java.text.ParseException;

public class Main {

	static final String APP_NAME = "BDM_P2";
	static final String PARQUET_FILE = "src/main/resources/idealista/2020_01_02_idealista/part-00000-73a2bf77-0d24-4f05-bf18-7351de8d7938-c000.snappy.parquet";

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


	public static void main(String[] args) throws Exception {

		// Data preparation
		// Idealista
		//JavaPairRDD<String, String> idealista = new IdealistaPreparation(IdealistaReader.allPairDateFilePath()).prepare(spark);
		//idealista.foreach(s -> System.out.println(s));

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
		//joinedIncome.foreach(s -> System.out.println(s));


		spark.close();



	}


}

