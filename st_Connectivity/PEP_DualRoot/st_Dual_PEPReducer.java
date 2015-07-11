/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class st_Dual_PEPReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Node outNode1 = new Node();
        Node outNode2 = new Node();
        boolean blackFound = false;
        boolean grayFound = false;
        
        String startPoint = "null";
        boolean stConnected = false;
        // set the node id as the key
        outNode1.setId(key.toString());
        outNode2.setId(key.toString());
           
        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());
           // context.write(key, new Text(inNode.getNodeInfo()));      
            // Emit one node after combining all the mapper outputs
            
            if (inNode.getColor() == Node.Color.BLACK) {
                blackFound = true;
                outNode2.setDistance(inNode.getDistance());
                outNode2.setColor(inNode.getColor());
                if(startPoint.equals("null")) {
                    outNode2.setStartPoint(inNode.getStartPoint());
                    startPoint = inNode.getStartPoint();
                }
                else if (!startPoint.equals(inNode.getStartPoint()))
                    stConnected = true;
            } 

            else if (inNode.getColor() == Node.Color.GRAY) {
                grayFound = true;
                if(!blackFound){
                    outNode1.setDistance(inNode.getDistance());
                    outNode1.setColor(inNode.getColor());
                }
                if(startPoint.equals("null")){
                	startPoint = inNode.getStartPoint();
                    outNode1.setStartPoint(inNode.getStartPoint());
                }
                else if (!startPoint.equals(inNode.getStartPoint())){
                	stConnected = true;
                }

            }
            // If its black, we dont add the adj list. If its gray, it will have a white counterpart
            // We dont have to worry about the adj list hack used in mapper since those were for GRAY
            else if (inNode.getColor() == Node.Color.WHITE && !blackFound) {
                outNode1.setEdges(inNode.getEdges());
            }
        }
        
        if(blackFound) {
            if(stConnected)
                context.getCounter(MoreIterations.bothBranchesMeet).increment(1);
             context.write(key, new Text(outNode2.getNodeInfo()));      
        }
        else { // WHITE OR GRAY
            if(stConnected)
                context.getCounter(MoreIterations.bothBranchesMeet).increment(1);
            context.write(key, new Text(outNode1.getNodeInfo()));  
            if(outNode1.getColor() == Node.Color.GRAY)        	
                context.getCounter(MoreIterations.numberOfIterations).increment(1);
        }
        // Even paths are distinguished by the presence of BLACK and GRAY pairs for the common nodes
        if(blackFound && grayFound && stConnected)
            context.getCounter(MoreIterations.evenPath).increment(1);
    }
}





