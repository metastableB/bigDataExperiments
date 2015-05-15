/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis [at] gmail.com
 *
 * Code part of an implementation of BFS using mapReduce Framerwork.
 * This code is inspired by the article here at :
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *
 * I/O: source<tab>adjacency_list|distance_from_the_source|color|parentNode
 *
 * The reducer cleans up the mappers output. It emits a new node for each key,
 * combining the outputs from the mapper jobs. The new node will have the id set,
 * the minimum distance, the darkest color, and its neighbours set.
 * Input format <key,value> : <nodeId, list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
 * Output format <key,value> : <nodeId, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
 *
 */
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class SearchReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
         
        // set the node id as the key
        outNode.setId(key.toString());
           
        // TODO : (huh?) Since the values are of the type Iterable, iterate through the values associated with the key
        // for all the values corresponding to a particular node id
   
        for (Text value : values) {
 
            Node inNode = new Node(key.toString() + "\t" + value.toString());
 
            // Emit one node after combining all the mapper outputs

            // Only one node(the original) will have a non-null adjascency list
            if (inNode.getEdges().size() > 0) {
                outNode.setEdges(inNode.getEdges());
            }
               
            // Save the minimum distance and parent
            if (inNode.getDistance() < outNode.getDistance()) {
                outNode.setDistance(inNode.getDistance());
                outNode.setParent(inNode.getParent());
            }
 
            // Save the darkest color
            if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
                outNode.setColor(inNode.getColor());
            }        
        }

        // Update the context object so that jobs can be informed about when to stop
        if (outNode.getColor() == Node.Color.GRAY)
            context.getCounter(MoreIterations.numberOfIterations).increment(1);
        context.write(key, new Text(outNode.getNodeInfo()));      
        //return outNode;
    }
}





