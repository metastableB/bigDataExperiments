package ex2_3_5;
/*
 * Don K Dennis (metastableB)
 * 1 May 2015
 * donkdennis@gmail.com
 *
 * The following code is part of my ventures into Hadoop and Big Data analysis.
 * This is intended to work on the climate data provided by NCDC.
 * The data is available here	http://www.ncdc.noaa.gov/
 *
 * Codes inspired from examples in Hadoop: The Definitive Guide
 * - Tom White - O'Reilly, 2nd Edition
 */
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass (MaxTemperature.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job ,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
