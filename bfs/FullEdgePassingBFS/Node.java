/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * Full Edges Passing BFS,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * I/O: source<tab>distance|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import java.util.*;
import org.apache.hadoop.io.Text;

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
        // tokens[0] = distance, tokens[1]= color, tokens[2]= Adjacency List
 
        this.id = key;
 		
        if (tokens[0].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            this.distance = Integer.parseInt(tokens[0]);
        }
        this.color = Color.valueOf(tokens[1]);

	    try {
	       	for (String s : tokens[2].split(",")) {
	            if (s.length() > 0) {
	                edges.add(s);
	            }
	        }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	System.exit(1);
	    }
    }
 
    // Recreating the I/O information format
    public Text getNodeInfo() {
    	StringBuffer s = new StringBuffer();      
        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance).append("|");
        } else {
            s.append("Integer.MAX_VALUE").append("|");
        }
        // append the color of the node
        s.append(color.toString()).append("|");
 		// append adjacency list
 		try {
            for (String v : edges) {
                s.append(v).append(",");
        	}
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }
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

	public void setId(String s) {
		this.id = s;
	}

	public void setDistance(int d) {
		this.distance = d;
	}

	public void setColor( Color s) {
		this.color = s;
	}

	public void setEdges(List<String> e) {
		for (String s : e)
			if(s.length() > 0)
				this.edges.add(s);
	}

}
 





