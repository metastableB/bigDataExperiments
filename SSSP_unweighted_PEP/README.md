# PEP Based Single Source Shortest Path on Hadoop

Here is an implementation of the standard BFS algorithm, modified
for the hadoop framework. The algorithm is able to run a BFS from a 
source specified at the command line. It is also able to run **Point To Point Shortes Path** by specifying the destination as well.

* Single Source Shortest Path With Unweighted Edes
* Point to Point Shortest Path With Unweighted Edges

### Usage
Assuming you packed the contents to a SSSP.jar file, run using

    $ hadoop jar SSSP.jar SSSP_UW_Job <inputPath> <outputPath> <JobName> <SourceVertex> [<DestinationVertex>]

The algorithm runs a PEP BFS on the nodes with <SourceVertex> as source. Level wise outputs are written to the output directory in corresponding folders. Once the BFS is complete the algorithm writes the path from source to <DestinationVertex> in a Path.txt file in the output directory if the <DestinationVertex] is specified.

### Input format
The program assumes the following input format
    
    NodeId<tab>DISTANCE|COLOR|PARENT|adj List

The initial inputs should contain initialized nodes in the following format:
  
    NodeId<tab>Integer.MAX_VALUE|WHITE|null|<adj List>

### Output

The explored nodes will be updated appropriately. For the Path.txt file written in PPSP, in case there exists a path, it will be written as shown here :
    
    source --> Node1 --> Node2 --> Node3

In case no path exists, the following will be written
 
    NO PATH

 &copy; IIIT Delhi , 2015.