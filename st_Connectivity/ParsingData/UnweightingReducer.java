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

public class UnweightingReducer extends Reducer<Text, Text, Text, Text> {
 
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        for (Text value : values) {
            context.write(key, value);
            context.write(value, key);             
        }
    }
}





