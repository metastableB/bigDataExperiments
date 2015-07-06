/*
 * Don K Dennis (metastableB)
 * 06 July 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance,start_point|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import java.util.*;
import org.apache.hadoop.io.Text;

// Defining a counter
enum MoreIterations {
    numberOfIterations, bothBranchesMeet
}


public class Node {
    public static enum Color {
        WHITE, GRAY, BLACK
    };
 
    private String id;
    private int distance;
    private List<String> edges = new ArrayList<String>();
    private Color color = Color.WHITE;
    private String startPoint = null;
    private String parent;
 
    public Node() {       
        edges = new ArrayList<String>();
        distance = Integer.MAX_VALUE;
        startPoint = null;
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
        // tokens[0] = distance,start_point, tokens[1]= color, tokens[2]= Adjacency List
 
        this.id = key;
 		String[] distance_tokens = tokens[0].split(",");
        if (distance_tokens[0].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            this.distance = Integer.parseInt(distance_tokens[0]);
        }
        this.startPoint = distance_tokens[1];
        this.color = Color.valueOf(tokens[1]);
        if(tokens.length == 3) {
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
    }
 
    // Recreating the I/O information format
    public Text getNodeInfo() {
    	StringBuffer s = new StringBuffer();      
        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance);
            s.append(this.startPoint).append("|");
        } else {
            s.append("Integer.MAX_VALUE");
            s.append(this.startPoint).append("|");
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

	public String getStartPoint() {
		return this.startPoint;
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
	public void setStartPoint(String s) {
		this.startPoint = s;
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
 





