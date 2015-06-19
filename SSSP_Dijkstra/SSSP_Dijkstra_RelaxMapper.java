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

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class SSSP_Dijkstra_RelaxMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Node inNode = new Node(value.toString());
        Configuration conf = context.getConfiguration();
        String param1 = conf.get("parent");
        
		if(inNode.getParent().equals(param1)) {
			for(String neighbour : inNode.getEdges()) {
				String[] elements = neighbour.split("*");
				Node adjNode = new Node();
				adjacentNode.setId(elements[0]); 
                adjacentNode.setDistance(inNode.getDistance() + Integer.valueOf(elements[1]));
                adjacentNode.setColor(Node.Color.GRAY);
                adjacentNode.setParent(inNode.getId());
                context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
			}
			inNode.setColor(Node.Color.BLACK);
			context.write(new Text(inNode.getId()), inNoe.getNNodeInfo());
		} else 
			context.write(new Text(inNode.getParent()),new Text(""));
    }
}



