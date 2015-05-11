import java.util.*;
import org.apache.hadoop.io.Text;
 
/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis@gmail.com
 *
 * Code part of an implementation of BFS using mapReduce Framerwork.
 * This code is inspired by the article here at :
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *
 * I/O: source<tab>adjacency_list|distance_from_the_source|color|parentNode
 *
 */

// Defining a counter
enum MoreIterations {
    numberOfIterations
}


public class Node {
   
    // Three possible colours a node can have
    // WHITE : not explored, GRAY : being explored . BLACK : Completely explored
    public static enum Color {
        WHITE, GRAY, BLACK
    };
 
    private String id;
    private int distance;
    private List<String> edges = new ArrayList<String>();
    private Color color = Color.WHITE;
    private String parent;
 
    public Node() {       
        edges = new ArrayList<String>();
        distance = Integer.MAX_VALUE;
        color = Color.WHITE;
        parent = null;
    }
 
    public Node(String nodeInfo) {
        // splitting the input line record by tab delimiter into key and value
        String[] inputLine = nodeInfo.split("\t");
        String key = "", value = ""; 
 
        try {
            key = inputLine[0]; 
            value = inputLine[1];
 
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
 		// String.split() uses regex, therefore escape |
        String[] tokens = value.split("\\|");
        // tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent
 
        this.id = key;
 
       for (String s : tokens[0].split(",")) {
            if (s.length() > 0) {
                edges.add(s);
            }
        }
 
        if (tokens[1].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            this.distance = Integer.parseInt(tokens[1]);
        }
 
        this.color = Color.valueOf(tokens[2]);
        this.parent = tokens[3];
    }
 
    // Recreating the I/O information format
    public Text getNodeInfo() {
    	// Remember strings are immutable in java
        StringBuffer s = new StringBuffer();
        try {
            for (String v : edges) {
                s.append(v).append(",");
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }
 
        s.append("|");
        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance).append("|");
        } else {
            s.append("Integer.MAX_VALUE").append("|");
        }
 
        // append the color of the node
        s.append(color.toString()).append("|");
 
        // append the parent of the node
        s.append(getParent());
 
        return new Text(s.toString());
    }

	public String getId() {
		return this.id;
	}

	public int getDistance() {
		return this.distance;
	}

	// Returns a csv of edges
	public List<String> getEdges() {
		return this.edges;
	}

	public Color getColor () {
		return this.color;
	}

	public String getParent () {
		return this.parent;
	}

	public void setId(String s) {
		this.id = s;
	}

	public void setDistance(int d) {
		this.distance = d;
	}

	public void setColor( Color s) {
		this.color = s;
	}

	public void setParent (String p) {
		this.parent = p;
	}
	
	public void setEdges(List<String> e) {
		for (String s : e)
			if(s.length() > 0)
				this.edges.add(s);
	}

}
 





