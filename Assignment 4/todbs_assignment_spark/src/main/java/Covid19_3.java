import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class Covid19_3 {

	public static void main(String[] args) throws IOException {
		Instant start = Instant.now();
		if (args.length != 3) {
			System.out.println("ERROR - Not Enough arguments!");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Full Path to populations.csv");
			System.out.println("args[2] - Output Directory");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("CasesPerMillion").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		Path delOutput = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(context.hadoopConfiguration());
		if (hdfs.exists(delOutput)) {
		    hdfs.delete(delOutput, true);
		}
		String path = args[1];
		Map<String,Double> populationMap = new HashMap<String, Double>();
		JavaRDD<String> lines = context.textFile(path);
        
        // collect RDD for printing
        for(String line:lines.collect()){
        	String[] temp = line.split(",");
        	if(temp.length < 5 || temp[0].equals("countriesAndTerritories")) {
        		continue;
        	}else {
        		populationMap.put(temp[1], Double.parseDouble(temp[4]));
        	}
        } 
		Broadcast<Map<String,Double>> ff = context.broadcast(populationMap);
		
		

		JavaRDD<String> textFile = context.textFile(args[0], 1).filter(line -> !line.startsWith("date"));
		JavaPairRDD<String, Double> pairs = textFile.mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String s) {
				String[] words = s.split(",");
				if(ff.getValue().getOrDefault(words[1], 0d) == 0d) {
					return new Tuple2<String,Double>(words[1],0d);
				}else {
					return new Tuple2<String,Double>(words[1],Double.parseDouble(words[2]) * 1000000/ff.getValue().get(words[1]));
				}
				
			}
		});
		
		JavaPairRDD<String, Double> counts = pairs.reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double a, Double b) {
				return a + b;
			}
		});
		
		counts.saveAsTextFile(args[2]);
		context.stop();
		context.close();
		Instant finish = Instant.now();
		long timeElapsed = Duration.between(start, finish).toMillis();
		System.out.println("Time taken - " + timeElapsed + "ms");
		
		
	}

}
