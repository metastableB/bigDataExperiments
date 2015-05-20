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

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class FEPMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Node inNode = new Node(value.toString());
 
        // For each GRAY node, emit each of the adjacent nodes as a new node
        // (also GRAY) if the adjacent node is already processed
        // and colored BLACK, the reducer retains the color BLACK
        // Note that the mapper is not differentiating between BLACK GREY AND WHITE

        if (inNode.getColor() == Node.Color.GRAY) {
            for (String neighbor : inNode.getEdges()) { 
                Node adjacentNode = new Node();
                List <String> s = new ArrayList <String> ();
                s.add(" ");
                // Remember that the current node only has the value the id 
                // of its neighbour, and not the object itself. Therefore at 
                // this stage there is no way of knowing and assigning any of
                // its other properties. Also remember that the reducer is doing
                // the 'clean up' task and not the mapper.
                adjacentNode.setId(neighbor); 
                adjacentNode.setDistance(inNode.getDistance() + 1);
                adjacentNode.setColor(Node.Color.GRAY);
                adjacentNode.setEdges(s);
                context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
            }
            inNode.setColor(Node.Color.BLACK);
        }
        // Emit the input node, other wise the BLACK color change(if it happens)
        // Wont be persistent and even if the node is WHITE, we need it for further
        // iterations.
        context.write(new Text(inNode.getId()), inNode.getNodeInfo());
    }
}



