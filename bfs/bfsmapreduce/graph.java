

import java.util.*;

public class Graph {

  private Map<Integer, Node> nodes;

  public Graph() {
    this.nodes = new HashMap<Integer, Node>();
  }

  public void breadthFirstSearch(int source) {

    // Set the initial conditions for the source node
    Node snode = nodes.get(source);
    snode.setColor(Node.Color.GRAY);
    snode.setDistance(0);

    Queue<Integer> q = new LinkedList<Integer>();
    q.add(source);

    while (!q.isEmpty()) {
      Node unode = nodes.get(q.poll());

      for (int v : unode.getEdges()) {
        Node vnode = nodes.get(v);
        if (vnode.getColor() == Node.Color.WHITE) {
          vnode.setColor(Node.Color.GRAY);
          vnode.setDistance(unode.getDistance() + 1);
          vnode.setParent(unode.getId());
          q.add(v);
        }
      }
      unode.setColor(Node.Color.BLACK);
    }

  }

  public void addNode(int id, int[] edges) {

    // A couple lines of hacky code to transform our
    // input integer arrays (which are most comprehensible
    // write out in our main method) into List<Integer>
    List<Integer> list = new ArrayList<Integer>();
    for (int edge : edges)
      list.add(edge);

    Node node = new Node(id);
    node.setEdges(list);
    nodes.put(id, node);
  }
 
  public void print() {
    for (int v : nodes.keySet()) {
      Node vnode = nodes.get(v);
      System.out.printf("v = %2d parent = %2d distance = %2d \n", vnode.getId(), vnode.getParent(),
          vnode.getDistance());
    }
  }

  public static void main(String[] args) {

    Graph graph = new Graph();
    graph.addNode(1, new int[] { 2, 5 });
    graph.addNode(2, new int[] { 1, 5, 3, 4 });
    graph.addNode(3, new int[] { 2, 4 });
    graph.addNode(4, new int[] { 2, 5, 3 });
    graph.addNode(5, new int[] { 4, 1, 2 });

    graph.breadthFirstSearch(1);
    graph.print();
  }

}