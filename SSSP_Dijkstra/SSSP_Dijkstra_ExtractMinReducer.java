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


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class SSSP_Dijkstra_ExtractMinReducer extends Reducer<Text, Text, Text, Text> {
     @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Integer min = Integer.MAX_VALUE;  
        String id="";       
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            Integer distance = Integer.valueOf(tokens[0]);
            
            if(min > distance) {
            	min = distance;
            	id = tokens[1];
            }
        }
        context.write(new Text(id),new Text(""));      
    }
}





