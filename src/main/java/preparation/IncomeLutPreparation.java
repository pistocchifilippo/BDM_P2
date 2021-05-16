package preparation;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncomeLutPreparation implements Preparation <String,String>{

    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("database", "lookup_tables");
        readOverrides.put("collection", "income_lut_neigh");
        readOverrides.put("readPreference.name", "secondaryPreferred"); // mirar millor (!!!)
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);


        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        JavaPairRDD<String, String> rdd2 = rdd.mapToPair((document) ->{
            String neigh = document.getString("neighborhood");
            String id = document.getString("_id");
            String neigh_reconciled = document.getString("neighborhood_reconciled");

            return new Tuple2<String, String>(neigh, id + "," + neigh_reconciled);

        });

        return rdd2;
    }
}