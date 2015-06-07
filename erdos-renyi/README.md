# Erdős–Rényi Graph Generator in MapReduce

This is a very basic implementation of a graph generator based on the G(n,p) model of Erdős–Rényi Graphs on the hadoop framework as a MapReduce Job.

The program uses two map-reduce jobs for execution. The first one populates the output directory with files containing the node names. The second map-reduce takes this as input and creates the adjacency list corresponding to the nodes based on the  G(n,p) model.

Usage

    hadoop jar JARFILE.jar Erdős–RényiJob <inpPath> <outPath> <JobName> <N> <P>
where,
* **JARFILE.jar** is the name of the jarfile, if the source code is packed in to jar.
* **inpPath** is the input path containing a file with the following format of input,

    
         <startNode><tab><endNode>
         <startNode><tab><endNode>
         
         # Example
         # 0	100
         # 101	200
         # 202	400
         # etc.
         
 * **outPath** the output directory. It will containing a subdirectory named *edges* with the required graph. The output format is as follows:
 
 
         <NodeId>	<comma separated edges list>
* **jobName** is a name identifier given to the job.
* **N** is the greatest node ID.
* **P** is the probability of edge existence.

 &copy;IIIT Delhi , 2015.
         
    