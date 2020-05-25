import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Covid19_1 {
	/*
	 * For Simplicity, We are creating custom Split function,so it makes code easier
	 * to understand We are implementing FlatMapFunction interface.
	 */
	
	static boolean toProcessOrNot(String line, boolean includeWorld) {
		String[] words = line.split(",");
		if(words[0] == "date") {
			return false;
		}else if(includeWorld == false) {
			if(words[1].toLowerCase().equals("world")) {
				return false;
			}else {
				return true;
			}
		}else {
			return true;
		}
	}

	static Date lastDate = new Date(120, 4, 8);
	static Date ignoreDate = new Date(119, 12, 31);

	public static void main(String[] args) {
		if(args.length != 3) {
			System.out.println("ERROR -  Wrong number of arguments!");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - To Include entries from 'World' or not (true/false)");
			System.out.println("args[2] - Output Directory");
			System.exit(1);
		}
		if(!args[1].equals("true") && !args[1].equals("false")) {
			System.out.println("ERROR -  args[1] is either true/false");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - To Include entries from 'World' or not (true/false)");
			System.out.println("args[2] - Output Directory");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Infected Population Counter");

		sparkConf.setMaster("local[1]");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> textFile = sparkContext.textFile(args[0]);

		JavaRDD<String> words = textFile.filter(line -> toProcessOrNot(line, args[1].equals("true") ? true : false));


		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] words = s.split(",");
				
					try {
						Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
						if(date1.getYear() > 119) {
							if (date1.before(lastDate) || date1.equals(lastDate)) {
								return new Tuple2<String, Integer>("Infected Population", new Integer(words[2]));
							}else {
								return new Tuple2<String, Integer>("Infected Population", 0);
							}
						}else {
							return new Tuple2<String, Integer>("Infected Population", 0);
						}
						

					} catch (Exception e) {
						return new Tuple2<String, Integer>("Infected Population", 0);
					}
				
			}
		});


		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		counts.saveAsTextFile(args[2]);
		sparkContext.stop();
		sparkContext.close();
	}

}
