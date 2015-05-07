/*
 * Don K Dennis (metastableB)
 * 7 May 2015
 * donkdennis@gmail.com
 *
 * Program to print BFS traversal from a given source vertex. BFS(int s) 
 *  
 */

#include <iostream>
#include <list>
 
using namespace std;
 
class Graph {
	//  no of vertices
	int V;
	// Adjacency list, int is the node key
	list <int> *adj;
public:
    Graph(int V);  
    void addEdge(int v, int w); // function to add an edge to graph
    void BFS(int s);  // prints BFS traversal from a given source s
};

Graph::Graph(int V) {
    this->V = V;
    adj = new list<int>[V];
}

// Adding (u,v) : adding v to u's adjacency list
void Graph::addEdge(int u, int v) {
    adj[u].push_back(v); 
    /* Debug */
    list<int>::iterator i;
    cout << "\nList of " << u << ": ";
    for(i = adj[u].begin(); i != adj[u].end(); ++i)
    	cout << *(i) << ", ";
    /*! Debug */
}

void Graph::BFS(int s) {
    // Mark all the vertices as not visited
    cout << "\nExploring : \n";
    bool *visited = new bool[V];
    for(int i = 0; i < V; i++)
        visited[i] = false;
 
    // Create a queue for BFS
    list<int> queue;
 
    // Mark the current node as visited and enqueue it
    visited[s] = true;
    queue.push_back(s);
 
    // 'i' will be used to get all adjacent vertices of a vertex
    list<int>::iterator i;
 
    while(!queue.empty()) {
        // Dequeue a vertex from queue and print it
        s = queue.front();
        cout << s << " ";
        queue.pop_front();
 
        // Get all adjacent vertices of the dequeued vertex s
        // If a adjacent has not been visited, then mark it visited
        // and enqueue it
        for(i = adj[s].begin(); i != adj[s].end(); ++i) {
            if(!visited[*i]) {
                visited[*i] = true;
                queue.push_back(*i);
            }
        }
    }
}

 
// Driver program to test methods of graph class
int main() {
    // Obtaining input as csv values
    int V = 0 , u,v,s;
    cout << "Usage: "
    		"		<no of Vertices V (numbered from 0)>\n"
    		" 		<space separated edges u,v or -1 to exit>\n"
    		"		<starting vertex>\n";	
    cin >> V;
    Graph g(V);
 	
 	for (int j = 0 ;  ; j++) {
 		cin >> u ;
 		if ( u == -1 )
 			break;
 		cin >> v ;
 		if ( v == -1 )
 			break;
 		g.addEdge(u,v);
 		
 	}
 	cin >> s;
    g.BFS(s);
    cout << "\n";
    return 0;
}
 