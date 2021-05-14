package preparation;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import javax.print.Doc;
import java.util.*;

public class IncomePreparation implements Preparation <String,String>{

    private final String path;


    public IncomePreparation(final String path) {
        this.path = path;
    }

    public JavaPairRDD<String, String> prepare(final SparkSession spark) {

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("database", "income_opendata");
        readOverrides.put("collection", "income");
        readOverrides.put("readPreference.name", "secondaryPreferred"); // mirar millor (!!!)
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);


        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, readConfig);

        // using MongoDB operators
        String pipeline = "{ $project: {'neigh_name ':1, info: {$filter: {input:'$info', as:'info', cond:{ $eq:['$$info.year', 2017] } } } } }";

        JavaRDD<Document> rdd2 = rdd.withPipeline(Arrays.asList(
                Document.parse(pipeline)));


        JavaPairRDD<String, String> rdd3 = rdd2.mapToPair((document) ->{
            String neigh = document.getString("neigh_name ");
            List<Document> info = (List<Document>) document.get("info");
            String rfd = info.get(0).getDouble("RFD").toString();
            String year = info.get(0).getInteger("year").toString();
            String pop = info.get(0).getInteger("pop").toString();
            String value =  year + "," + pop + "," + rfd;

            return new Tuple2<String, String>(neigh, value);

        });






        //System.out.println(rdd3.take(10));

/*
        JavaRDD<List<String>> rdd2 = rdd.map((document) ->{
            String neigh = document.getString("neigh_name ");
            List<Document> info = (List<Document>) document.get("info");

            int i;
            List<String> value = new ArrayList<>();;
            for (i = 0; i <= info.size() - 1; ++i) {

                String rfd = info.get(i).getDouble("RFD").toString();

                String year = info.get(i).getInteger("year").toString(); // nomes selecciona el 2011

                String pop = info.get(i).getInteger("pop").toString();

                String data = neigh + ";" + year + ";" + pop + ";" + rfd;

                value.add(data);
            }
            return value;

        });
        System.out.println(rdd2.first());
*/
        //JavaRDD<String> rdd3 = rdd2.flatMap(s -> { return Arrays.asList(s.subList()))});

        //JavaRDD<String> rdd3 = rdd2.filter(s -> s.contains("Raval"));
        //System.out.println(rdd3.take(4));
        //JavaPairRDD<String, String> rdd4 = rdd3.mapToPair(s -> new Tuple2<>(s.split(";")[0], s));


        return rdd3;
    }
}
