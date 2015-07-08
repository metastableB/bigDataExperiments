/*
 * Don K Dennis (metastableB)
 * 09 July 2015
 * donkdennis [at] gmail.com
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class DivideConquerMaxMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Random generator = new Random(); 
		int i = generator.nextInt(Integer.MAX_VALUE);
        String k = conf.get("k");
        String iterationCount = conf.get("iterationCount");
        if(iterationCount.equals("0")){
        	String id = String.valueOf((i % Integer.valueOf(k)));
        	context.write(new Text(id), value);
        }
        else {
        	String[] tokens = (value.toString()).split("\t");
        	String id = String.valueOf((i % Integer.valueOf(k)));
        	context.write(new Text(id),new Text(tokens[1]));
        }
    }
}



