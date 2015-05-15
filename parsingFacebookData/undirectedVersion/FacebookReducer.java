/*
 * Don K Dennis (metastableB)
 * 16 May 2015
 * donkdennis [at] gmail.com
 *
 * Parsing input from the SNAP facebook dataset
 * into files with values of the form 
 * node comma separated list of neighbours
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * http://snap.stanford.edu/data/egonets-Facebook.html
 */
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;

public class FacebookReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // We dont have to do anything specific here, just write the entries to the file
        // In the format required.
        StringBuffer neighbours = new StringBuffer();
	  	for (Text value : values) {
	  		neighbours.append(value.toString()).append(", ");
       	}
        context.write(key, new Text(neighbours.toString()));      
        //return outNode;
    }
}





