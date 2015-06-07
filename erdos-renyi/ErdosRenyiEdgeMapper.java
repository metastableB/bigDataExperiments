 /*
 * Don K Dennis (metastableB)
 * 07 June 2015
 * donkdennis [at] gmail.com
 *
 * Erdos-Renyi Edge Mapper
 *
 * Node<tab>
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class ErdosRenyiEdgeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line= value.toString().split("\t");
        String nodeId = line[0];

        Integer start = 0;
        Configuration conf = context.getConfiguration();
        String param1 = conf.get("end");
        String param2 = conf.get("probability");
        Integer end = Integer.parseInt(param1);
        Double probability = Double.parseDouble(param2);

        StringBuffer adjList = new StringBuffer();
        for (int i = 0; i <= end; i++) 
            if(Math.random() <= probability )
                adjList.append(String.valueOf(i)).append(",");

        context.write(new Text((String.valueOf(nodeId))),new Text(adjList.toString()));
       }
    
}



