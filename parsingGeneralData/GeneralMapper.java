import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
 
/*
 * Don K Dennis (metastableB)
 * 23 May 2015
 * donkdennis [at] gmail.com
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * (c) IIIT Delhi, 2015
 */

 
// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class GeneralMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            	
        String valueString = new String(value.toString());
        // Split at space
        String[] nodes = valueString.split("\t"); 
        context.write(new Text(nodes[0]), new Text(nodes[1]));
        
    }
}



