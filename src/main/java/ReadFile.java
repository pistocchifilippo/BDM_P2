import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFile {

    public static String basicAnalysis(JavaSparkContext ctx) {
        String out = "";

        JavaRDD<String> ds = ctx.textFile("src/main/resources/income_opendata/income_opendata_neighborhood.json");

        ds.foreach(line -> System.out.println(line));

        return out;
    }
}
