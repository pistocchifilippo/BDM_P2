package preparation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class IdealistaPreparation implements Preparation <String,String>{

    private static final int PROPERTY_CODE_INDEX = 20;
    private static final String SEPARATOR = ",";
    private final Stream<Tuple2<String,String>> paths;

    public IdealistaPreparation(final Stream<Tuple2<String,String>> paths) {
        this.paths = paths;
    }


    static String getAttr(List<String> schemaFields, String s, Row row){
        if (schemaFields.contains(s)) {
            try {
                String neigh = row.getAs(s).toString();
                return neigh;
            } catch (NullPointerException e) {}
        }
        else {
            //System.out.println("Attribute not exists");
            return "null";
        }
        return "null";
    }


    static String selectAttr(Row row){
        List<String> schemaFields = Arrays.stream(row.schema().fieldNames()).collect(Collectors.toList());
        //System.out.println(schemaFields);
        String propertyCode = getAttr(schemaFields, "propertyCode", row);
        String neigh = getAttr(schemaFields, "neighborhood", row);
        String price = getAttr(schemaFields, "price", row);
        String val = propertyCode + SEPARATOR + neigh + SEPARATOR +price;
        return val;
    }


    @Override
    public JavaPairRDD<String, String> prepare(final SparkSession spark){

        JavaRDD<String> emptyRDD = new JavaSparkContext(spark.sparkContext()).emptyRDD();

        // paths.forEach(s -> System.out.println(s));
        JavaPairRDD<String, String> rdd1 = paths.map(t -> new Tuple2<>(t._1, spark.read().parquet(t._2).toJavaRDD()))
                .map(t -> t._2.map(row -> {
                    String value = selectAttr(row);
                    return t._1 + SEPARATOR + value;
                }))
                .reduce(emptyRDD, (a, b) -> a.union(b))
                .mapToPair(l -> new Tuple2<>(l.split(SEPARATOR)[1],l))
                .reduceByKey((String a, String b) -> (a.split(SEPARATOR)[0].compareTo(b.split(SEPARATOR)[0])) < 0 ? a : b)
                .mapToPair(l -> new Tuple2<>(l._2.split(SEPARATOR)[1],l._2))
                .mapToPair(l -> new Tuple2<>(l._2.split(SEPARATOR)[2],l._2));

        return rdd1;
    }
}

         //       .reduceByKey((String a, String b) -> (a.split(SEPARATOR)[0].compareTo(b.split(SEPARATOR)[0])) < 0 ? a : b)


//     .reduceByKey((String a, String b) ->{
//                    //Date date1 = sdf.parse(a.split(SEPARATOR)[0].replace("_", "-"));
//                    String result = compareDate(a,b);
//                  //  System.out.println(result);
//                    return result;
//                //    Date date2 = sdf.parse(b.split(SEPARATOR)[0].replace("_", "-"));
//                //    if (date1.compareTo(date2) < 0) return a; else return b;
//                } )