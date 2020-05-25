import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Covid19_2 {
	public static int validateInputs(String startDate, String endDate) {

		try {
			Date stDate = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
			Date eDate = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
			if (stDate.before(eDate) || stDate.equals(eDate)) {
				return 0;
			} else {
				return 1;
			}
		} catch (IllegalArgumentException iae) {
			return 2;
		}catch(ParseException pe) {
			return 2;
		}

	}
	
	static boolean toProcessOrNot(String line, String startDate, String endDate) {
		Date stDate;
		Date eDate;
		try {
			stDate = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
			eDate = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
			String[] words = line.split(",");
			if(words[0].equals("date")) {
				return false;
			}else {
				Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
				if (date1.equals(stDate) || date1.equals(eDate) || (date1.after(stDate) && date1.before(eDate))) {
					return true;
				}else {
					return false;
				}
			}
		}catch(Exception e) {
			return false;
		}
	}

	public static void main(String[] args) throws IOException {
		Instant start = Instant.now();
		if (args.length != 4) {
			System.out.println("ERROR - Not Enough arguments!");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Start Date");
			System.out.println("args[2] - End Date");
			System.out.println("args[3] - Output Directory in HDFS");
			System.exit(1);

		}
		int isValid = validateInputs(args[1], args[2]);

		if (isValid == 1) {
			System.out.println("ERROR - Start Date must be lesser than End Date");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Start Date");
			System.out.println("args[2] - End Date");
			System.out.println("args[3] - Output Directory in HDFS");
			System.exit(1);

		} else if (isValid == 2) {
			System.out.println("ERROR - Date Format is yyyy-MM-dd");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Start Date");
			System.out.println("args[2] - End Date");
			System.out.println("args[3] - Output Directory in HDFS");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Death Counter");
		sparkConf.setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		Path delOutput = new Path(args[3]);
		FileSystem hdfs = FileSystem.get(sparkContext.hadoopConfiguration());
		if (hdfs.exists(delOutput)) {
		    hdfs.delete(delOutput, true);
		}
		JavaRDD<String> textFile = sparkContext.textFile(args[0]);
		JavaRDD<String> words = textFile.filter(line -> toProcessOrNot(line, args[1],args[2]));
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] words = s.split(",");
				return new Tuple2<String,Integer>(words[1],new Integer(words[3]));
			}
		});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		counts.saveAsTextFile(args[3]);
		sparkContext.stop();
		sparkContext.close();
		Instant finish = Instant.now();
		long timeElapsed = Duration.between(start, finish).toMillis();
		System.out.println("Time taken - " + timeElapsed + "ms");
	}
}
