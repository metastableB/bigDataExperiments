 /*
 * Don K Dennis (metastableB)
 * 19 June 2015
 * donkdennis [at] gmail.com
 *
 * Finding the SSSP using Dijkstra 
 * Inp: source<tab>distance|color|parent|adjacency list with wights (csv)
 * Out: source<tab>distance|color|parent|<adj list>
 *
 * (c) IIIT Delhi, 2015
 */


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class SSSP_Dijkstra_RelaxReducer extends Reducer<Text, Text, Text, Text> {
     @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
        boolean hasWhite = false , hasGray = false;
         
        // set the node id as the key
        outNode.setId(key.toString());
           
        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());
            // Emit one node after combining all the mapper outputs
            
            if (inNode.getColor() == Node.Color.BLACK) {
            	if(outNode.getDistance() > inNode.getDistance())
                	outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                break;
            } 
            else if (inNode.getColor() == Node.Color.GRAY) {
            	if(outNode.getDistance() > inNode.getDistance())
                	outNode.setDistance(inNode.getDistance());
                outNode.setColor(inNode.getColor());
                outNode.setEdges(inNode.getEdges());
                hasGray = true;
            }
            else if (inNode.getColor() == Node.Color.WHITE) {
                outNode.setEdges(inNode.getEdges());
                hasWhite = true;
            }
        }
        // Update the context object so that jobs can be informed about when to stop
        if (outNode.getColor() == Node.Color.GRAY && hasWhite)
            context.getCounter(MoreIterations.numberOfIterations).increment(1);

        context.write(key, new Text(outNode.getNodeInfo()));      
    }
}





