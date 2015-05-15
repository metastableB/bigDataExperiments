import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
 
/*
 * Don K Dennis (metastableB)
 * 16 May 2015
 * donkdennis [at] gmail.com
 *
 * Parsing input from the SNAP facebook dataset
 * into files with values of the form 
 * node list of neighbours
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * http://snap.stanford.edu/data/egonets-Facebook.html
 */

 
// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class FacebookMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            	
        String valueString = new String(value.toString());
        // Split at space
        String[] nodes = valueString.split(" "); 
        context.write(new Text(nodes[0]), new Text(nodes[1]));
 
    }
}



