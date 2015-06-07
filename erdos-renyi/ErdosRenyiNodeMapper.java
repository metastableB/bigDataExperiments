 /*
 * Don K Dennis (metastableB)
 * 06 June 2015
 * donkdennis [at] gmail.com
 *
 * Erdos-Renyi Mapper
 *
 * startNode<tab>endNode
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class ErdosRenyiNodeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line= value.toString().split("\t");
        Integer start = Integer.parseInt(line[0]);
        Integer end = Integer.parseInt(line[1]);

        for (int i = start; i <= end ; i++)
        	context.write(new Text((String.valueOf(i))),new Text(""));
    }
}



