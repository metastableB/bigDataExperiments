import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
 
/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis@gmail.com
 *
 * Code part of an implementation of BFS using mapReduce Framerwork.
 * This code is inspired by the article here at :
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *
 * I/O: source<tab>adjacency_list|distance_from_the_source|color|parentNode
 *
 * The mapper explores all the nodes at the current level. For grey node, it 
 * emmits a copy of all the nodes adjoining. The new nodes emited have only
 * their ID, distance and colour set. The reducer does the job of cleaning up
 * multiple occurrences thus created and setting their values. 
 *
 */

 
// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class SearchMapper extends Mapper<Object, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    public void map(Object key, Text value, Context context, Node inNode)
            throws IOException, InterruptedException {
 
        // For each GRAY node, emit each of the adjacent nodes as a new node
        // (also GRAY) if the adjacent node is already processed
        // and colored BLACK, the reducer retains the color BLACK
        // Note that the mapper is not differentiating between BLACK GREY AND WHITE

        if (inNode.getColor() == Node.Color.GRAY) {
            for (String neighbor : inNode.getEdges()) { 
                Node adjacentNode = new Node();
               
                // Remember that the current node only has the value the id 
                // of its neighbour, and not the object itself. Therefore at 
                // this stage there is no way of knowing and assigning any of
                // its other properties. Also remember that the reducer is doing
                // the 'clean up' task and not the mapper.
                adjacentNode.setId(neighbor); 
                adjacentNode.setDistance(inNode.getDistance() + 1);
                adjacentNode.setColor(Node.Color.GRAY);
                adjacentNode.setParent(inNode.getId());
                context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
            }
            inNode.setColor(Node.Color.BLACK);
        }
        // Emit the input node, other wise the BLACK color change(if it happens)
        // Wont be persistent 
        context.write(new Text(inNode.getId()), inNode.getNodeInfo());
 
    }
}



