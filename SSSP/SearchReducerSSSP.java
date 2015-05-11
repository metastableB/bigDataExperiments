/**
 *
 * Description : Reducer class that implements the reduce part of the Single-source shortest path algorithm. This class extends the SearchReducer class that implements parallel breadth-first search algorithm.
 *      The reduce method implements the super class' reduce method and increments the counter if the color of the node returned from the super class is GRAY.  
 *
 *      
 */
 
    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type
import org.apache.hadoop.io.Text;
import java.io.IOException;
public class SearchReducerSSSP extends SearchReducer{
 
 
    //the parameters are the types of the input key, the values associated with the key and the Context object through which the Reducer communicates with the Hadoop framework
 
   
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
 
           //create a new out node and set its values
            Node outNode = new Node();
           
            //call the reduce method of SearchReducer class
            outNode = super.reduce(key, values, context, outNode);                                     
 
            //if the color of the node is gray, the execution has to continue, this is done by incrementing the counter
            if (outNode.getColor() == Node.Color.GRAY)
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);
    }
}
 

