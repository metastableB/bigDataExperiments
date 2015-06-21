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

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class SSSP_Dijkstra_ExtractMinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Node inNode = new Node(value.toString());
        Integer distance = inNode.getDistance();
        if(inNode.getColor() == Node.Color.GRAY)
            context.write(new Text("1"),new Text(distance.toString() + "," + inNode.getId()));
    }
}



