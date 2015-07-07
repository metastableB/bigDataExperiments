/*
 * Don K Dennis (metastableB)
 * 06 July 2015
 * donkdennis [at] gmail.com
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.util.*;

public class ListCreatorReducer extends Reducer<Text, Text, Text, Text> {
 
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
        outNode.setId(key.toString());
        ArrayList<String> edges = new ArrayList<>();
        for (Text value : values) {
             edges.add(value.toString());
        }
        outNode.setEdges(edges);
        context.write(key, outNode.getNodeInfo());
    }
}





