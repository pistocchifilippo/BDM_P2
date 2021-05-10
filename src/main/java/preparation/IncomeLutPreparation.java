package preparation;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.util.List;

public class IncomeLutPreparation implements Preparation <String,String>{

    private final String path;

    public IncomeLutPreparation(final String path) {
        this.path = path;
    }

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println(rdd.take(5));
        JavaPairRDD<String, String> rdd2 = rdd.mapToPair((document) ->{
            String neigh = document.getString("neighborhood");
            String id = document.getString("_id");
            //String name = document.getString("neighborhood_name");
            //String re = document.getString("neighborhood_reconciled");
            //String value = neigh + ";" + id + ";" + name + ";" + re;

            return new Tuple2<String, String>(neigh, id);

        });

        return rdd2;
    }
}
