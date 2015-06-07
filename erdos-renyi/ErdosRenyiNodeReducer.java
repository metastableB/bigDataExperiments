/*
 * Don K Dennis (metastableB)
 * 6 June 2015
 * donkdennis [at] gmail.com
 * 
 * Erdos-Renyi Graph generator
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ErdosRenyiNodeReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
                  context.write(key, new Text(""));      
    }
}





