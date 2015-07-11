/*
 * Don K Dennis (metastableB)
 * 10 Jul 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
public class st_Triple_PEPReducer extends Reducer<Text, Text, Text, Text> {
 
    // Types of the input key, the values associated with the key, the Context object for Reducers communication
    // with the Hadoop framework and the node whose information has to be output
    // the return type is a Node
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String s = conf.get("s");
        String t = conf.get("t");
        String u = conf.get("u");
        Node outNode1 = new Node();
        Node outNode2 = new Node();
        boolean s_found = false, t_found = false, u_found = false;
        boolean s_black = false, t_black = false, u_black = false;
        boolean s_gray = false, t_gray = false, u_gray = false;
        boolean blackFound = false;
        outNode1.setId(key.toString());
        outNode2.setId(key.toString());

        Integer minDistance = Integer.MAX_VALUE;
        Integer tempDistance = Integer.MAX_VALUE;
        /*ArrayList<Text> cache = new ArrayList<Text>();
        for(Text value : values) {
            cache.add(value);
            context.write(new Text(key.toString() +"cac"),value);
            String[] tokens2 = value.toString().split("\\|");
            String[] tokens3 = tokens2[0].split(",");
            if(tokens3[0].equals("Integer.MAX_VALUE"))
                tempDistance = Integer.MAX_VALUE;
            else 
                tempDistance = Integer.valueOf(tokens3[0]);
            if(tempDistance <= minDistance)
                minDistance = tempDistance;
        }*/

        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());

            tempDistance = inNode.getDistance();
            if(tempDistance < minDistance && inNode.getColor() == Node.Color.BLACK) {
                minDistance = tempDistance;
                s_black = false;
                t_black = false;
                u_black = false;
                s_gray = false;
                t_gray = false;
                u_gray = false;
                s_found = false;
                t_found = false;
                u_found = false;
                outNode1.reset();
                outNode2.reset();
                blackFound = false;
            } else {
                //context.write(new Text(key.toString() +"dum"),value);
                //(inNode.getDistance() > minDistance)
                  //  continue;
                // if distance = mindistance 
                if (inNode.getColor() == Node.Color.BLACK) {
                    if(inNode.getStartPoint().equals(s)) {
                        s_black = true;
                        s_found = true;
                    }
                    else if (inNode.getStartPoint().equals(t)){
                        t_found = true;
                        t_black = true;
                    }
                    else {
                        u_found = true;
                        u_black = true;
                    }
                    outNode2.setDistance(inNode.getDistance());
                    outNode2.setColor(inNode.getColor());
                    outNode2.setStartPoint(inNode.getStartPoint());
                    blackFound = (s_black || u_black || t_black);
                } 
                else if (inNode.getColor() == Node.Color.GRAY) {
                    if(inNode.getStartPoint().equals(s)) {
                        s_gray = true;
                        s_found = true;
                    }
                    else if (inNode.getStartPoint().equals(t)){
                        t_found = true;
                        t_gray = true;
                    }
                    else {
                        u_found = true;
                        u_gray = true;
                    }
                    if(!blackFound){
                        outNode1.setDistance(inNode.getDistance());
                        outNode1.setColor(inNode.getColor());
                    }
                        outNode1.setStartPoint(inNode.getStartPoint());
                }
                else if (inNode.getColor() == Node.Color.WHITE && !blackFound ) {
                    outNode1.setEdges(inNode.getEdges());
                }
            }
        }
        
        if(blackFound) {
            context.write(key, new Text(outNode2.getNodeInfo()));      
            if(s_found && t_found)
                context.getCounter(MoreIterations.stMeet).increment(1);
            if(s_found && u_found)
                context.getCounter(MoreIterations.suMeet).increment(1);
            if(t_found && u_found)
                context.getCounter(MoreIterations.utMeet).increment(1);
        }
        else { // WHITE OR GRAY
            if(s_found && t_found)
                context.getCounter(MoreIterations.stMeet).increment(1);
            if(s_found && u_found)
                context.getCounter(MoreIterations.suMeet).increment(1);
            if(t_found && u_found)
                context.getCounter(MoreIterations.utMeet).increment(1);
            context.write(key, new Text(outNode1.getNodeInfo()));      
            if(outNode1.getColor() == Node.Color.GRAY)	
                context.getCounter(MoreIterations.numberOfIterations).increment(1);
        }
        // Even paths are distinguished by the presence of BLACK and GRAY pairs for the common nodes
        if((( s_black && t_gray) || (t_black && s_gray)) && (s_found) && (t_found))
            context.getCounter(MoreIterations.stEvenPath).increment(1);
        if((( s_black && u_gray) || (u_black && s_gray)) && (s_found) && (u_found))
            context.getCounter(MoreIterations.suEvenPath).increment(1);
        if((( u_black && t_gray) || (t_black && u_gray)) && (u_found) && (t_found))
            context.getCounter(MoreIterations.utEvenPath).increment(1);
    }
}





