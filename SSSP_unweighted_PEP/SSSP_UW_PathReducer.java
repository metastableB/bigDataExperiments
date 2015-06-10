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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class SSSP_UW_PathReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            	for (Text value : values)
        			context.write(key, new Text(""));      
    }
}





