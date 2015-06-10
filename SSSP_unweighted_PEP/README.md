# PEP Based Single Source Shortest Path on Hadoop

Here is an implementation of the standard BFS algorithm, modified
for the hadoop framework. The algorithm is able to run a BFS from a specified
source. The source being specified at the command line. 

Optionally, it can also print the path from the source till a specified destination,
if the destination is specified as well as a command line argument.

### Usage
Assuming you packed the contents to a SSSP.jar file, run using

    $ hadoop jar SSSP.jar SSSP_UW_Job <inputPath> <outputPath> <JobName> <SourceVertex> <DestinationVertex>

The algorithm runs a PEP BFS on the nodes with <SourceVertex> as source. Level wise outputs are written to the output directory in corresponding folders. Once the BFS is complete the algorithm writes the path from source to <DestinationVertex> in a Path.txt file in the output directory.

### Input format
The program assumes the following input format
    
    NodeId<tab>DISTANCE|COLOR|PARENT|adj List

The initial inputs should contain initialized nodes in the following format:
  
    NodeId<tab>Integer.MAX_VALUE|WHITE|null|<adj List>

