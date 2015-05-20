/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis@gmail.com
 *
 * Program to print BFS traversal from a given source vertex. BFS(int s) 
 *  
 */

import java.util.Scanner;
import java.util.ArrayList;


class Graph {
	private int V;
	private ArrayList <ArrayList<Integer> > adjList ;

	Graph(int v) {
		this.V = v;
		adjList = new ArrayList <ArrayList<Integer>> (this.V);

		for(int i = 0 ; i < this.V ; i++) {
			adjList.add( new ArrayList<Integer>() );
		}
	}

	public boolean addEdge(Integer u , Integer v) {
		return this.adjList.get(u).add(v);
	}

	public void printAdjList (Integer u) {
		System.out.print("AdjList of "+u+" : ");
		for (Integer v : this.adjList.get(u)) 
			System.out.print(v+", ");
		System.out.print("\n");
	}

	// BFS with s as source vertex
	public void BFS(Integer s) {

		// Keeping track of the visited vertices
		ArrayList <Boolean> visited = new ArrayList<Boolean>(this.V);
		for ( int i = 0 ; i < this.V ; i++ ) 
			visited.add(i , false);// = false;
		
		ArrayList <Integer> queue = new ArrayList<Integer> ();
		queue.add(s);
		visited.set(s, true);// = true;

		Integer currentVertex ;
		System.out.println("Exploring");
		while( !queue.isEmpty()) {
			currentVertex = queue.get(0);
			queue.remove(0);
			System.out.print( " --> " +currentVertex );
			for (Integer u : this.adjList.get(currentVertex)) {
				if ( visited.get(u) != true) {
					queue.add(u);
					visited.set(u , true);// = true;
				}
			}
		}
		System.out.print("\n");
	} 
}