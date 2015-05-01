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
 * Sample data entry :

0029029070999991901010513004+64333+023450FM-12+000599999V0202701N002119999999N0000001N9+00061+99999102591ADDGF104991999999999999999999

 *
 * Codes inspired from examples in Hadoop: The Definitive Guide
 * - Tom White - O'Reilly, 3rd Edition
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
    /* LongWritable, Text , IntWritable are data types provided by the 
     * hadoop library. They are optimised for parellel handling. Essentially
     * they are simillar to Long, int and string .
     * Think QString, QIntiger , QLongIntiger in Qt/C++
     */

	/* The NCDC data set used the flag 9999 for missing temperatures */
	private static final int MISSING = 9999;
	
	/* This class extends the Mapper interface. It has the map() function 
	 * declaration.The function accepts four parameters as shown below (the 
	 * class is generic so these parameters can be of any type of choice - see
	 * class declaration for clarification), the first being the key to the
	 * data entry, the next the text value, the third is an output collector 
	 * (context) which will be used to shuffle and reduce.
	 */
	@Override
	public void map(LongWritable key, Text value,
		Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		
		if (line.charAt(87) == '+') {
		// parseInt doesn't like leading plus signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		
		String quality = line.substring(92, 93);
		/* Determines the quality of the database entry, refer to 
		 * the website for the meaing of the entries */
		
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}	
	}
}
