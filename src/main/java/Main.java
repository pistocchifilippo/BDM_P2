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

	public static void main(String[] args) throws Exception {

		IdealistaReader.allFilesName().forEach(f -> System.out.println(f));
		IdealistaReader.allFilesPath().forEach(f -> System.out.println(f));
		IdealistaReader.allPairDateFilePath().forEach(f -> System.out.println(f));



		// Try to read and clean data separately at first
		JavaRDD<Row> parquetFileDF = spark.read().parquet(PARQUET_FILE).javaRDD();
		parquetFileDF
				.mapToPair(e -> new Tuple2<>("THE DATE",e))
				.foreach(e -> System.out.println(e));

	}


}

