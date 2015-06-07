/*
 * Don K Dennis (metastableB)
 * 07 June 2015
 * donkdennis [at] gmail.com
 * 
 * Erdos-Renyi Edge Reducer
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ErdosRenyiEdgeReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            int i = 0;
            for (Text t :values)  {
            	i++;
            	if(i == 1) 
                 	context.write(key, t);      
		    }
	}
}





