import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import preparation.BuildingAgePreparation;
import preparation.IdealistaPreparationNoDuplicates;

public class Main {

	static final String APP_NAME = "BDM_P2";
	static final SparkSession spark = SparkSession
			.builder()
			.appName(APP_NAME)
			.master("local[*]")
			.getOrCreate();

	static final String INCOME_LUT = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
	static final String RENT_LUT = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";
	static final String AGE_DATASET = "src/main/resources/building_age/2020_edificacions_edat_mitjana.csv";
	static final String INCOME_DATASET = "src/main/resources/income_opendata/income_opendata_neighborhood.json";

	public static void main(String[] args) {

		final JavaPairRDD<String, String> oldness = new BuildingAgePreparation(AGE_DATASET).prepare(spark);

//		oldness.foreach(e -> System.out.println(e));

		final JavaPairRDD<String, String> idealista = new IdealistaPreparationNoDuplicates(IdealistaReader.allPairDateFilePath()).prepare(spark);

		idealista.foreach(e -> System.out.println(e));

	}

}

