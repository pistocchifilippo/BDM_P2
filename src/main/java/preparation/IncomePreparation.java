package preparation;

import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

public class IncomePreparation implements Preparation <String, String> {

    private final String path;

    public IncomePreparation(final String path) {
        this.path = path;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        spark.read().textFile(path).javaRDD()
                .map(e -> new JSONObject(e).get("info"))
                .foreach(e -> System.out.println(e));

        return null;
    }
}
