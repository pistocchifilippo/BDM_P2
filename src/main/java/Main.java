import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	static final String APPLICATION_NAME = "BDM_P2";
	
	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName(APPLICATION_NAME).setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

		ReadFile.basicAnalysis(ctx);

	}
}

