# s-t Connectivity Test

This is an implementation to test the connectivity between two nodes s and t, implemented using a PEP Breadth First Search ( [here](https://github.com/metastableB/bigDataExperiments/tree/master/bfs/PartialEdgePassingBFS)), on hadoop.

The basic idea is to run simultaneous BFS jobs from both **s** and **t** till at least one node is found common to both the BFS.

## Notes

We require adjacency list representation. The input files should contains lines in the following format :

    <NodeId>   Integer.MAX_VALUE,null|WHITE|<csv edges>

Where **Interger.MAX_VALUE** signifies an  infinite distance, **null** is a place holder but required. It is used to test for convergence of both the BFS. **WHITE** is the initial color and the **<csv edges>** hold the comma separated values of edge.

Since we are using adjacency list, for evey edge **(u,v)**, **v** should be in the adjacency list of **u** and **u** in the list of **v**.

Further **s** and **t** should not be same and both values should exist in the set.

## Usage

Compile using regular hadoop compilation techniques and pack the classes in to a .jar. Use to following format to run

    hadoop jar st_Dual_PEPJob.jar st_Dual_PEPJob <input files path> <output file path> <a name for the job> <s> <t>

Visit to find out more.
{Detailed description soon}

&copy;IIIT Delhi , 2015.