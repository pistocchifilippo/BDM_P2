import KPIs.KPI1;
import KPIs.KPI2;
import KPIs.KPI3;
import KPIs.MyKPIs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.INT;
import preparation.*;
import scala.Tuple2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;

public class Main {

	static final String APP_NAME = "BDM_P2";

	static final SparkSession spark = SparkSession
			.builder()
			.appName(APP_NAME)
			.master("local[*]")
			.config("spark.mongodb.input.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.config("spark.mongodb.output.uri", "mongodb://10.4.41.153/lookup_tables.income_lut_neigh")
			.getOrCreate();
	private static MyKPIs output;


	public static void main(String[] args) throws Exception {

		if (args[0].equals("-retrieve")) {

			if (args[1].equals("-KPI1")) {
				output = (MyKPIs) new KPI1();
				JavaRDD<String> result = output.retrieve(spark);
				result.foreach(s -> System.out.println(s));
				//result.coalesce(1).saveAsTextFile(args[2]);
				spark.close();
			} else if (args[1].equals("-KPI2")) {
				output = (MyKPIs) new KPI2();
				JavaRDD<String> result = output.retrieve(spark);
				result.coalesce(1).saveAsTextFile(args[2]);
				spark.close();
			} else if (args[1].equals("-KPI3")) {
			output = (MyKPIs) new KPI3();
			JavaRDD<String> result = output.retrieve(spark);
			result.coalesce(1).saveAsTextFile(args[2]);
			spark.close();
		}
	}




	}}




