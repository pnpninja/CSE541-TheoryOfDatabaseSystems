import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	public static void main(String[] args) throws Exception {
		Instant start = Instant.now();
		if (args.length != 4) {
			System.out.println("ERROR - Not Enough arguments!");
			System.out.println("args[0] - Input Directory in HDFS");
			System.out.println("args[1] - Start Date");
			System.out.println("args[2] - End Date");
			System.out.println("args[3] - Output Directory in HDFS");
			System.exit(1);

		}
		Configuration c = new Configuration();
		Path delOutput = new Path(args[3]);
		FileSystem hdfs = FileSystem.get(c);
		if (hdfs.exists(delOutput)) {
		    hdfs.delete(delOutput, true);
		}
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(args[0]);
		Path output = new Path(args[3]);
		int isValid = validateInputs(args[1], args[2]);

		if (isValid == 1) {
			System.out.println("ERROR - Start Date must be lesser than End Date");
			System.exit(1);

		} else if (isValid == 2) {
			System.out.println("ERROR - Date Format is yyyy-MM-dd");
			System.exit(1);
		}

		c.set("startDate", args[1]);
		c.set("endDate", args[2]);
		Job j = new Job(c, "death-count");
		j.setJarByClass(Covid19_2.class);
		j.setMapperClass(MapForDeathCount.class);
		j.setReducerClass(ReduceForDeathCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
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

	public static class MapForDeathCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		static Date lastDate = new Date(120, 4, 8);
		static Date ignoreDate = new Date(119, 12, 31);
		static Log logger = LogFactory.getLog(MapForDeathCount.class);
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			//System.out.println("Herererere" + con.getConfiguration().get("startDate"));
			Date stDate;
			Date eDate;
			try {
				stDate = new SimpleDateFormat("yyyy-MM-dd").parse(con.getConfiguration().get("startDate"));
				eDate = new SimpleDateFormat("yyyy-MM-dd").parse(con.getConfiguration().get("endDate"));
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return;
			}
			
			String line = value.toString();
			String[] words = line.split(",");
			if (words[0].equals("date")) {
				return;
			} else {
				try {
					Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
					
						if (date1.equals(stDate) || date1.equals(eDate) || (date1.after(stDate) && date1.before(eDate))) {
							Text outputKey = new Text(words[1]);
							IntWritable outputValue = new IntWritable(new Integer(words[3]));
							con.write(outputKey, outputValue);
						}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
	}

	public static class ReduceForDeathCount extends Reducer<Text, IntWritable, Text, Text> {
		public void run(Context con) throws IOException, InterruptedException {
			setup(con);
			String startDate = con.getConfiguration().get("startDate");
			String endDate = con.getConfiguration().get("endDate");
			//con.write(new Text("Start Date - "), new Text(startDate));
			//con.write(new Text("End Date - "), new Text(endDate));
			try
	        {
	          while (con.nextKey())
	          {
	            reduce(con.getCurrentKey(), con.getValues(), con);
	            Iterator<IntWritable> iter = con.getValues().iterator();
	            if(iter instanceof ReduceContext.ValueIterator)
	            {              ((ReduceContext.ValueIterator<IntWritable>)iter).resetBackupStore();        
	            }
	          }
	        }
	        finally
	        {
	          cleanup(con);
	        }
			
			
		}
		
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(word, new Text(Integer.toString(sum)));
		}
	}
}
