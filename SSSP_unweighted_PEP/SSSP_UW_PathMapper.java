 /*
 * Don K Dennis (metastableB)
 * 10 June 2015
 * donkdennis [at] gmail.com
 *
 * Finding the SSSP for a unweighed graph using 
 * the Partial Edges Passing BFS (PEP) algorithm,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * Inp: source<tab>distance|color|parent|adjacency list (csv)
 * Out: source<tab>distance|color|parent|<adj list>
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
public class SSSP_UW_PathMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Node inNode = new Node(value.toString());
        Configuration conf = context.getConfiguration();
        String param1 = conf.get("source");
        boolean blackAndSource = (inNode.getColor() == Node.Color.BLACK) ;
		blackAndSource = blackAndSource	&& (inNode.getId().equals(param1));
		if(blackAndSource)
        	context.write(new Text(inNode.getParent()),new Text(""));//new Text(inNode.getId()),
    }
}



