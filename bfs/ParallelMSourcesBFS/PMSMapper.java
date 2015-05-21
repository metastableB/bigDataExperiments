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
        bool flag = false;
        Node inNode = new Node(value.toString());
 
        for (Node.Color color : inNode.getColor()) {
            if (color == Node.Color.GRAY) {
                flag = true;
                break;
            }
        }
        int numberOfGray = 0;
        // If there is atleast one gray node, explore all adj list
        if ( flag == true ) {
            for (String v : inNode.getEdges()) {
                if (v.size() > 0) {
                    // Initialize an empty white nodes
                    Node outNode = new Node(inNode.getNumberOfSources());
                    outNode.setId(v);
                    // set GRAY according to inpNode
                    int i = 0;
                    for(Node.Color c : inpNode.getColor()) {
                        if (c == Node.Color.GRAY) {
                            outNode.setColorOf(i,Node.Color.GRAY);
                            outNode.setDistanceOf(i,inpNode.getDistance() + 1);
                        }
                        i++;
                    }
                    context.write( new Text(outNode.getId()),outNode.getNodeInfo());
                }
            }
            // Write back the current node with GRAY turned to black
            int i = 0;
            for (Node.Color c : inpNode.getColor()) {
                if(c == Node.Color.GRAY) {
                    inpNode.setColorOf(i,Node.Color.Black);
                    numberOfGray++ ;
                }
                i++;
            }
        }
        //Decrease the number of gray nodes
        context.getCounter(MoreIterations.numberOfIterations).increment( -1*(numberOfGray) );
        context.write(new Text(inpNode.getId()) , inpNode.getNodeInfo());
    }
}



