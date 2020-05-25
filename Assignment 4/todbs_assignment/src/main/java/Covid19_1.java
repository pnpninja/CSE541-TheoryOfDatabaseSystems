import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Month;
import java.time.Year;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Covid19_1 {
	public static void main(String[] args) throws Exception {
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
		Configuration c = new Configuration();
		Path delOutput = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(c);
		if (hdfs.exists(delOutput)) {
		    hdfs.delete(delOutput, true);
		}
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(args[0]);
		Path output = new Path(args[2]);
		Job j = new Job(c, "infected-population-count");
		j.setJarByClass(Covid19_1.class);
		if(args[1].toLowerCase().equals("true")){
			j.setMapperClass(MapForWordCountWithWorld.class);
		}else{
			j.setMapperClass(MapForWordCountWithoutWorld.class);
		}		
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWordCountWithWorld extends Mapper<LongWritable, Text, Text, IntWritable> {
		static Date lastDate = new Date(120, 4, 8);
		static Date ignoreDate = new Date(119, 12, 31);
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			if (words[0].equals("date")) {
				return;
			} else {
				try {
					Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
					if(date1.getYear() > 119) {
						if (date1.before(lastDate) || date1.equals(lastDate)) {
							Text outputKey = new Text(words[1]);
							IntWritable outputValue = new IntWritable(new Integer(words[2]));
							con.write(outputKey, outputValue);
						}
					}
					

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
	}

		public static class MapForWordCountWithoutWorld extends Mapper<LongWritable, Text, Text, IntWritable> {
		static Date lastDate = new Date(120, 4, 8);
		static Date ignoreDate = new Date(119, 12, 31);
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			if (words[0].equals("date")) {
				return;
			}else if(!words[1].toLowerCase().equals("world")){
				try {
					Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
					if(date1.getYear() > 119) {
						if (date1.before(lastDate) || date1.equals(lastDate)) {
							Text outputKey = new Text(words[1]);
							IntWritable outputValue = new IntWritable(new Integer(words[2]));
							con.write(outputKey, outputValue);
						}
					}
					

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}

