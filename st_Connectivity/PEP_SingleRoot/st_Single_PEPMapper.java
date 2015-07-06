/*
 * Don K Dennis (metastableB)
 * 06 July 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance,start_point|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class st_Single_PEPMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Node inNode = new Node(value.toString());
        Configuration conf = context.getConfiguration();
        String s = conf.get("s");
        if(conf.get("iterationCount").equals("0")){
        	if(inNode.getId().equals(s)){
        		inNode.setStartPoint(s);
        		inNode.setDistance(0);
            inNode.setColor(Node.Color.GRAY);
        	}
        }
 
	    if (inNode.getColor() == Node.Color.GRAY) {
            for (String neighbor : inNode.getEdges()) { 
                Node adjacentNode = new Node();
                adjacentNode.setId(neighbor); 
                adjacentNode.setDistance(inNode.getDistance() + 1);
                adjacentNode.setStartPoint(inNode.getStartPoint());
                adjacentNode.setColor(Node.Color.GRAY);
                context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
            }
            inNode.setColor(Node.Color.BLACK);
        }
        context.write(new Text(inNode.getId()), inNode.getNodeInfo());
    }
}



