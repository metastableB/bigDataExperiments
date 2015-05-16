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

public class bfsDriver {
 public static void main (String args[]) {
    System.out.println("Usage: ");
	System.out.println(	"		<no of Vertices V (numbered from 0)>\n");
	System.out.println(	" 		<space separated edges u,v or -1 to exit>\n");
	System.out.println(	"		<starting vertex>\n");
	// Timing total execution time
	long startTime = System.nanoTime();

    Integer u , v ,s;
    Scanner inp = new Scanner(System.in);		
    v = inp.nextInt();
    if( v <= 0) {
    	System.out.println( v);
    	System.out.println ( "ERROR : non Integer value\n");
    	//return 0;
    }
	Graph G = new Graph(v);

	for (int j = 0 ;  ; j++) {
			u = inp.nextInt();
 		if ( u == -1 )
 			break;
 		v = inp.nextInt();
			if ( v == -1 )
				break;
			if( !G.addEdge(u,v) ) {
				System.out.println("Could not add Edge\n");
				break;
			}
	 	}
	
	s = inp.nextInt();
	long startTimeBFS = System.nanoTime();
	G.BFS(s);
	long endTimeBFS = System.nanoTime();
	long durationBFS = endTimeBFS - startTimeBFS;


	long endTime = System.nanoTime();
	long duration = (endTime - startTime);

    System.out.println("Execution Time BFS : " + durationBFS + "ns (" + (double)durationBFS/1000000 + "ms)");
    System.out.println("Execution Time Total : " + duration + "ns (" + (double)duration/1000000 + "ms)");
    }
}