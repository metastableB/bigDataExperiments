/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * Full Edges Passing BFS,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * I/O: source<tab>distance|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class FEPReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
         
        // set the node id as the key
        outNode.setId(key.toString());
           
        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());
            // Emit one node after combining all the mapper outputs
            
            if (inNode.getColor() == Node.Color.BLACK) {
                outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                outNode.setEdges(inNode.getEdges());
                break;
            } 

            else if (inNode.getColor() == Node.Color.GRAY) {
                outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
            } // The second part of the if condition is so that the hack edge
              // from mapper is not added after an adj list is added
            else if (inNode.getEdges().size() > 0 &&
             outNode.getEdges().size() < inNode.getEdges().size()) {
                outNode.setEdges(inNode.getEdges());
            }
        }
        // Update the context object so that jobs can be informed about when to stop
        if (outNode.getColor() == Node.Color.GRAY)
            context.getCounter(MoreIterations.numberOfIterations).increment(1);

        context.write(key, new Text(outNode.getNodeInfo()));      
    }
}





