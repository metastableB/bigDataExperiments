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
        PMSNode outNode = new PMSNode();
        // We use this flag to initialize the outNode correctly
        boolean allocaedFlag = false;
        int numberOfGray=0;
      
        for (Text value : values) {
            PMSNode inNode = new PMSNode(key.toString() + "\t" + value.toString());
            if(allocaedFlag == false) {
                outNode = new PMSNode(inNode.getNumberOfSources());
                outNode.setId(key.toString());
                allocaedFlag = true;
            }
            
            int i = 0;
            for(PMSNode.Color color : inNode.getColor()) {
                if(color == PMSNode.Color.BLACK) {
                    outNode.setColorOf(i,PMSNode.Color.BLACK);
                    outNode.setDistanceOf(i,inNode.getDistanceOf(i));
                }
                else if (color == PMSNode.Color.GRAY) {
                    if (outNode.getColorOf(i) == PMSNode.Color.WHITE)
                        outNode.setColorOf(i,PMSNode.Color.GRAY);
                    if (outNode.getDistanceOf(i) > inNode.getDistanceOf(i))
                        outNode.setDistanceOf(i,inNode.getDistanceOf(i));
                }
                i++;
            }
            if(inNode.getEdges().size() > 0)
                outNode.setEdges(inNode.getEdges());
        }
        for(PMSNode.Color color : outNode.getColor()) 
                if (color == PMSNode.Color.GRAY) 
                    numberOfGray++;
        // The counter before the mapers start is zero, by incrementing
        // we are flagging the presence of more gray nodes to explore
        context.getCounter(MoreIterations.numberOfIterations).increment((numberOfGray));
        context.write(key, new Text(outNode.getNodeInfo()));  
    }
}

