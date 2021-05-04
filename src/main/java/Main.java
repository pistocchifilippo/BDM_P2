import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Main {

	static final String APP_NAME = "BDM_P2";
	static final String PARQUET_FILE = "src/main/resources/idealista/2020_01_02_idealista/part-00000-73a2bf77-0d24-4f05-bf18-7351de8d7938-c000.snappy.parquet";
	static final SparkSession spark = SparkSession
			.builder()
			.appName(APP_NAME)
			.master("local[*]")
			.getOrCreate();

	static final String LUT1 = "src/main/resources/lookup_tables/income_lookup_neighborhood.json";
	static final String LUT2 = "src/main/resources/lookup_tables/rent_lookup_neighborhood.json";

	public static void main(String[] args) throws Exception {

		JavaPairRDD<String, String> a = spark.read().json(LUT1).javaRDD()
				.map(e -> e.toString())
				.mapToPair(e -> new Tuple2<>(e.split(",")[0], e.split(",")[1]));

		JavaPairRDD<String, String> b = spark.read().json(LUT2).javaRDD()
				.map(e -> e.toString())
				.mapToPair(e -> new Tuple2<>(e.split(",")[0], e.split(",")[1]));





	}


}

