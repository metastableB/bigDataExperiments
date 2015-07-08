/*
 * Don K Dennis (metastableB)
 * 06 July 2015
 * donkdennis [at] gmail.com
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class DivideConquerMaxReducer extends Reducer<Text, Text, Text, Text> {
 
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        Integer max = -1;
        for (Text value : values) {
        	if(Integer.valueOf(value.toString()) > max)
        		max = Integer.valueOf(value.toString());
        }
        context.write(key, new Text(max.toString()));
        context.getCounter(NumberOfLines.numberOfLines).increment(1);
    }
}





