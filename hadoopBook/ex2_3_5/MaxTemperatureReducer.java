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
 * - Tom White - O'Reilly, 3rd Edition
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxTemperatureReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
		Context context)
		throws IOException, InterruptedException {
		
		int maxValue = Integer.MIN_VALUE;
		for(IntWritable value : values) {
			maxValue = Math.max(maxValue,value.get());
		}
		
		context.write(key, new IntWritable(maxValue));
	}
}

