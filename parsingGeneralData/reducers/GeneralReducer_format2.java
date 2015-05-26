/*
 * Don K Dennis (metastableB)
 * 23 May 2015
 * donkdennis [at] gmail.com
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * Output : Format 2
 * <node><tab><distance>|<color>|<adj list>
 *
 * (c) IIIT Delhi, 2015 
 */
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;

public class GeneralReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // We dont have to do anything specific here, just write the entries to the file
        // In the format required.
        StringBuffer neighbours = new StringBuffer();
	  	
        if ( key.toString().equals("0")){
            neighbours.append("|");
            neighbours.append("0").append("|");
            neighbours.append("GRAY").append("|");
                  
        } else {
            neighbours.append("|");
            neighbours.append("Integer.MAX_VALUE").append("|");
            neighbours.append("WHITE").append("|");
        }
        try {
            for (Text v : values) {
                if(!v.toString().equals("-1"))
                    neighbours.append(v.toString()).append(",");
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        context.write(key, new Text(neighbours.toString()));      
        
    }
}





