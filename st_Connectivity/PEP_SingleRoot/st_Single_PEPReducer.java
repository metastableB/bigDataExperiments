/*
 * Don K Dennis (metastableB)
 * 06 July 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance,start_point|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class st_Single_PEPReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
        Configuration conf = context.getConfiguration();
        String t = conf.get("t");
        // set the node id as the key
        outNode.setId(key.toString());
           
        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());
            // Emit one node after combining all the mapper outputs
            
            if (inNode.getColor() == Node.Color.BLACK) {
                outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                outNode.setStartPoint(inNode.getStartPoint());
                break;
            } 

            else if (inNode.getColor() == Node.Color.GRAY) {
                outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                outNode.setStartPoint(inNode.getStartPoint());
            }
            
            else if (inNode.getColor() == Node.Color.WHITE) {
                outNode.setEdges(inNode.getEdges());
            }
        }
        // Update the context object so that jobs can be informed about when to stop
        if (outNode.getColor() == Node.Color.GRAY) 
            context.getCounter(MoreIterations.numberOfIterations).increment(1);
        
        context.write(key, new Text(outNode.getNodeInfo()));     

        if(outNode.getId().equals(t) && outNode.getColor() != Node.Color.WHITE)
        	context.getCounter(MoreIterations.bothBranchesMeet).increment(1);
       
    }
}





