 /**
     *
     * Description : Mapper class that implements the map part of Single-source shortest path algorithm. It extends the SearchMapper   class.
     *  The map method calls the super class' map method.
     *  Input format : nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent_node
     *
     *      
     * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
     *
     *      
     */
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class SearchMapperSSSP extends SearchMapper {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
   
        Node inNode = new Node(value.toString());
        //calls the map method of the super class SearchMapper
        super.map(key, value, context, inNode);

    }
}
 
