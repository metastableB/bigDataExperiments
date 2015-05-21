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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PMSReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode = new Node();
         
        // set the node id as the key
        outNode.setId(key.toString());
        int numberOfGray = 0;

        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());
            
            int i = 0;
            for(Node.Color color : inNode.getColor()) {
                if(color == Node.Color.BLACK) {
                    outNode.setColorOf(i,Node.Color.BLACK);
                    outNode.setDistanceOf(i,inNode.getDistanceOf(i));
                }
                else if (color == Node.Color.GRAY) {
                    if (outNode.getColorOf(i) == Node.Color.WHITE){
                        outNode.setColorOf(i,Node.Color.GRAY);
                        numberOfGray++;
                    }
                    if (outNode.getDistanceOf(i) > inNode.getDistanceOf(i))
                        outNode.setDistanceOf(i,inNode.setDistanceOf(i));
                }
            }
            if(inNode.getEdges().size() > 0)
                outNode.setEdges(inNode.getEdges());
        }

        context.getCounter(MoreIterations.numberOfIterations).increment(numberOfGray);
        context.write(key, new Text(outNode.getNodeInfo()));  

    }
}

