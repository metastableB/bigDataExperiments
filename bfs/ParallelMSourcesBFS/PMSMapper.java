/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * Parallel M source BFS,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * I/O: source<tab>csv of distances | csv of colors| csv of adjacency list 
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// The type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class PMSMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    // Types of the input key, input value and the Context object through which 
    // the Mapper communicates with the Hadoop framework
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        boolean flag = false;
        PMSNode inNode = new PMSNode(value.toString());
 
        for (PMSNode.Color color : inNode.getColor()) {
            if (color == PMSNode.Color.GRAY) {
                flag = true;
                break;
            }
        }

        // If there is atleast one gray node, explore all adj list
        // Give a little thought to the logic here, we are not keeping
        // track of the path. Just the distance from any of the sources
        // and the mimimum of them
        if ( flag == true ) {
            for (String v : inNode.getEdges()) {
                if (v.length() > 0) {
                    // Initialize an empty white nodes
                    PMSNode outNode = new PMSNode(inNode.getNumberOfSources());
                    outNode.setId(v);
                    // set GRAY according to inNode
                    int i = 0;
                    for(PMSNode.Color c : inNode.getColor()) {
                        if (c == PMSNode.Color.GRAY) {
                            outNode.setColorOf(i,PMSNode.Color.GRAY);
                            outNode.setDistanceOf(i,inNode.getDistanceOf(i) + 1);
                           // outNode.setEdges(hack);
                        }
                        i++;
                    }
                    context.write( new Text(outNode.getId()),outNode.getNodeInfo());
                }
            }
            // Write back the current node with GRAY turned to black
            int i = 0;
            for (PMSNode.Color c : inNode.getColor()) {
                if(c == PMSNode.Color.GRAY) {
                    inNode.setColorOf(i,PMSNode.Color.BLACK);
                }
                i++;
            }
        }
        
        context.write(new Text(inNode.getId()) , inNode.getNodeInfo());
    }
}



