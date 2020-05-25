import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Covid19_3 {
	public static void main(String[] args) throws Exception {
		Instant start = Instant.now();
		if (args.length != 3) {
			System.out.println("ERROR - Not Enough arguments!");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Path to populations.csv");
			System.out.println("args[2] - Output Directory");
			System.exit(1);
		}
		Configuration c = new Configuration();
		Path delOutput = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(c);
		if (hdfs.exists(delOutput)) {
		    hdfs.delete(delOutput, true);
		}
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(args[0]);
		Path output = new Path(args[2]);
		c.set("popFileLoc", args[1]);
		Job j = new Job(c, "cases-per-million");
		j.addCacheFile(new Path(args[1]).toUri());
		DistributedCache.addCacheFile(new Path(args[1]).toUri(), c);
		j.setJarByClass(Covid19_3.class);
		j.setMapperClass(MapForCasesPerMillion.class);
		j.setReducerClass(ReduceForCasesPerMillion.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		boolean isJobComplete = j.waitForCompletion(true);
		if(isJobComplete) {
			Instant finish = Instant.now();
			long timeElapsed = Duration.between(start, finish).toMillis();
			System.out.println("Time taken - " + timeElapsed + "ms");
			System.exit(0);
		}else {
			System.exit(1);
		}
	}

	public static class MapForCasesPerMillion extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		static Log logger = LogFactory.getLog(MapForCasesPerMillion.class);

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			Date stDate;
			Date eDate;

			String line = value.toString();
			String[] words = line.split(",");
			if (words[0].equals("date")) {
				return;
			} else {
				Text outputKey = new Text(words[1]);
				//System.out.println(words[1]);
				DoubleWritable outputValue = new DoubleWritable(new Double(words[2]));
				con.write(outputKey, outputValue);

			}

		}
	}

	public static class ReduceForCasesPerMillion extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		static Map<String, Double> populationMap = new HashMap<String, Double>();
		
		@Override
		public void setup(Context context) {
			try {
				URI[] files = context.getCacheFiles();
				for (URI file : files){ 
					Path pt = new Path(file.getPath());
					//System.out.println(file.getPath());
					FileSystem fs = FileSystem.get(context.getConfiguration());
					FSDataInputStream fsDataInputStream = fs.open(pt);
					BufferedReader br=new BufferedReader(new InputStreamReader(fsDataInputStream));					
					try {
						String line = br.readLine();
						line = br.readLine();
						while(line != null) {
							String[] val = line.split(",");
							if(val.length < 5) {
								line = br.readLine();
							}else {
								this.populationMap.put(val[1], new Double(val[4]));
								line = br.readLine();
							}							
						}
						//System.out.println(Arrays.deepToString(populationMap.keySet().toArray()));
					}catch(Exception e){
						e.printStackTrace();
					}finally {
						br.close();
					}
			    }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				for(URI p : DistributedCache.getCacheFiles(context.getConfiguration())){
					//System.out.println(p.toString());
					FileSystem fs = FileSystem.get(context.getConfiguration());
					FSDataInputStream fsDataInputStream = fs.open(new Path(p.toString()));
					BufferedReader br=new BufferedReader(new InputStreamReader(fsDataInputStream));			
					//BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					try {
						String line = br.readLine();
						line = br.readLine();
						while(line != null) {
							String[] val = line.split(",");
							if(val.length < 5) {
								line = br.readLine();
							}else {
								this.populationMap.put(val[1], new Double(val[4]));
								line = br.readLine();
							}							
						}
						//System.out.println(Arrays.deepToString(populationMap.keySet().toArray()));
					}catch(Exception e){
						e.printStackTrace();
					}finally {
						br.close();
					}
				}
				
			}catch(Exception e) {
				e.printStackTrace();
			}
			
		}
		public void reduce(Text word, Iterable<DoubleWritable> values, Context con)
				throws IOException, InterruptedException {
			double avg = 0;
			for (DoubleWritable value : values) {
				avg += value.get();
			}
			//System.out.println(this.populationMap.get("Kazakhstan"));
			//System.out.println(word);
			if(this.populationMap.containsKey(word.toString())) {
				//System.out.println("Country - "+word);
				//System.out.println("Infected Pop - "+avg);
				//System.out.println("Total Pop - "+this.populationMap.getOrDefault(word.toString(), 0.0));
				con.write(word, new DoubleWritable(avg * 1000000/this.populationMap.get(word.toString())));
			}
			
		}
	}
}
