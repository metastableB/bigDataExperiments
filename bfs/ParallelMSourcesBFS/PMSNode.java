/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * Parallel M Source BFS,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * I/O: source<tab>csv of distances | csv of colors| csv of adjacency list 
 *
 * (c) IIIT Delhi, 2015
 */

import java.util.*;
import org.apache.hadoop.io.Text;

// Defining a counter
enum MoreIterations {
    numberOfIterations
}


public class PMSNode {
   
    // Three possible colours a node can have
    // WHITE : not explored, GRAY : being explored . BLACK : Completely explored
    public static enum Color {
        WHITE, GRAY, BLACK
    };
 
    private String id;
    private List <Integer> distances = new ArrayList<Integer>();;
    private List<String> edges = new ArrayList<String>();
    private List<Color> colors = new ArrayList<Color>();
     
    public PMSNode(){
    	edges = new ArrayList<String>();
    	colors = new ArrayList<Color> ();
    	distances = new ArrayList<Integer>();
    }

    public PMSNode(int noOfSources) {       
        edges = new ArrayList<String>();
        for (int i = 0 ; i < noOfSources ; i++) {
            distances.add(Integer.MAX_VALUE);
            colors.add(Color.WHITE);
       	}
    }

 
    public PMSNode(String nodeInfo) {
        // splitting the input line record by tab delimiter into key and value
        String[] inputLine = nodeInfo.split("\t");
        String key = "", value = ""; 
        // tokens[0] = distanceList, tokens[1]= colorList, tokens[2]= Adjacency List
 
        try {
            key = inputLine[0]; 
            value = inputLine[1];
 
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
 		// String.split() uses regex, therefore escape |
        String[] tokens = value.split("\\|");
        this.id = key;
 		
 		// Adding the distances. Beware there is no checking for spaces or tabs
        try {
        	for (String s : tokens[0].split(",")) {
	     	   if (s.equals("Integer.MAX_VALUE")) {
	        	    this.distances.add(Integer.MAX_VALUE);
	        	} // To be safe against null strings after last comma (,) 
	        	else if(s.length() > 0 ){
	            	this.distances.add(Integer.parseInt(s));
	        	}
	        }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	System.exit(2);
	    }
	    // Adding colors
	    try {
	    	for (String s : tokens[1].split(",")) {
	    		if (s.equals("WHITE"))
	    			this.colors.add(Color.WHITE);
	    		else if (s.equals ("GRAY"))
	    			this.colors.add(Color.GRAY);
	    		else if (s.equals("BLACK"))
	    			this.colors.add(Color.BLACK);

	      	}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	System.exit(3);
	    }

	    // Adding adj list
	    if (tokens.length == 3) {
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
    	// Append distances
    	for (Integer d : this.distances){
	        if (d < Integer.MAX_VALUE) {
	            s.append(d).append(",");
	        } else {
	            s.append("Integer.MAX_VALUE").append(",");
	        }
	    }
	    
	    s.append("|");

        // append the color of the node
    	for (Color d : this.colors){
	        s.append(d.toString()).append(",");
	    }

        s.append("|");
 		
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

	public List<Integer> getDistance() {
		return this.distances;
	}

	public List<String> getEdges() {
		return this.edges;
	}

	public List<Color> getColor () {
		return this.colors;
	}

	public int getNumberOfSources() {
		// Number of original source wil be the number of distances
		return this.distances.size();
	}

	public Color getColorOf(int i) {
		return this.colors.get(i);
	}

	public Integer getDistanceOf(int i) {
		return this.distances.get(i);
	}

	public void setId(String s) {
		this.id = s;
	}

	public void setDistanceIntegerList(List<Integer> d) {
		for (Integer i : d)
			this.distances.add(i);
	}

	public void setDistance(List<String> d) {
		for (String i : d)
			if(i.length() > 0)
				this.distances.add(Integer.parseInt(i));
	}
	public void setColor( List<Color> s) {
		for(Color c : s)
			this.colors.add(c);
	}

	public void setEdges(List<String> e) {
		for (String s : e)
			if(s.length() > 0)
				this.edges.add(s);
	}

	public void setColorOf(int i, Color c) {
		this.colors.set(i,c);
	}

	public void setDistanceOf(int i, Integer d) {
		this.distances.set(i,d);
	}
}
 





