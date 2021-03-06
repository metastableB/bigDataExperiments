/*
 * Don K Dennis (metastableB)
 * 09 June 2015
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

public class SSSP_UW_PEPReducer extends Reducer<Text, Text, Text, Text> {
 
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
                outNode.setParent(inNode.getParent());
                break;
            } 

            else if (inNode.getColor() == Node.Color.GRAY) {
                outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                outNode.setParent(inNode.getParent());
            }
            // If its black, we dont add the adj list. If its gray, it will have a white counterpart
            // We dont have to worry about the adj list hack used in mapper since those were for GRAY
            else if (inNode.getColor() == Node.Color.WHITE) {
                outNode.setEdges(inNode.getEdges());
            } 
        }
        // Update the context object so that jobs can be informed about when to stop
        if (outNode.getColor() == Node.Color.GRAY)
            context.getCounter(MoreIterations.numberOfIterations).increment(1);

        context.write(key, new Text(outNode.getNodeInfo()));      
    }
}





