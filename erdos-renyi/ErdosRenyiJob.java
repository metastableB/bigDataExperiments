/*
 * Don K Dennis (metastableB)
 * 21 May 2015
 * donkdennis [at] gmail.com
 *
 * Partial Edges Passing BFS,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * I/O: source<tab>distance|color|adjacency list (csv)
 *
 * (c) IIIT Delhi, 2015
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class ErdosRenyiJob extends Configured implements Tool  {
    // the driver to execute the job and invoke the map/reduce functions

    @Override
    public int run(String[] args) throws Exception {
        long  runningTime, startTime, endTime, totalRunningTime = 0;
        String nodeCreatorJobName = new String(args[2]+ "_nodeCreator");
        
        Job nodeCreatorJob = new Job(getConf());
        nodeCreatorJob.setJarByClass(getClass());
        nodeCreatorJob.setJobName(nodeCreatorJobName);
        String input, output;
        input = args[0];
        output = args[1]+"/nodes/";
        FileInputFormat.setInputPaths(nodeCreatorJob, new Path(input));
        FileOutputFormat.setOutputPath(nodeCreatorJob, new Path(output));
        
        nodeCreatorJob.setMapperClass(ErdosRenyiNodeMapper.class);
 	   	//nodeCreatorJob.setCombinerClass(Combiner.class);
    	nodeCreatorJob.setReducerClass(ErdosRenyiNodeReducer.class);

    	nodeCreatorJob.setOutputKeyClass(Text.class);
    	nodeCreatorJob.setOutputValueClass(Text.class);
     
        startTime = System.nanoTime();
        nodeCreatorJob.waitForCompletion(true); 
        endTime = System.nanoTime();
        runningTime = endTime - startTime;
        totalRunningTime += runningTime;

        System.out.println("=====================================================================");
        System.out.println ("Job Running Time = " + runningTime);
        //terminationValue =  nodeCreatorJob.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
        System.out.println("=====================================================================");
   
        // SECOND JOB Here
        String edgeCreatorJobName = new String(args[2]+ "_edgeCreator");
        Configuration conf = new Configuration();
		conf.set("end", args[3]);
		conf.set("probability",args[4]);
		
        Job edgeCreatorJob = new Job(conf);
        edgeCreatorJob.setJarByClass(getClass());
        edgeCreatorJob.setJobName(edgeCreatorJobName);
        
        input = args[1]+"/nodes/";
        output = args[1]+"/edges/";
        FileInputFormat.setInputPaths(edgeCreatorJob, new Path(input));
        FileOutputFormat.setOutputPath(edgeCreatorJob, new Path(output));
        
        edgeCreatorJob.setMapperClass(ErdosRenyiEdgeMapper.class);
 	   	//edgeCreatorJob.setCombinerClass(Combiner.class);
    	edgeCreatorJob.setReducerClass(ErdosRenyiEdgeReducer.class);

    	edgeCreatorJob.setOutputKeyClass(Text.class);
    	edgeCreatorJob.setOutputValueClass(Text.class);
     
        startTime = System.nanoTime();
        edgeCreatorJob.waitForCompletion(true); 
        endTime = System.nanoTime();
        runningTime = endTime - startTime;
        totalRunningTime += runningTime;
        //terminationValue =  edgeCreatorJob.getCounters().findCounter(MoreIterations.numberOfIterations).setValue();
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("=====================================================================");
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 5){
            System.err.println("Usage: <in> <output name> <jobName> <LastNode> <probability>");
            System.exit(1);
        }
        int res = ToolRunner.run(new ErdosRenyiJob(), args);
        System.exit(res);
    }

    public static String getRunningTime( long nanoTime) {
        long x = nanoTime / 1000000000;
        long seconds = x % 60;
        x /= 60;
        long minutes = x % 60 ;
        x /= 60;
        long hours = x % 24;
        return Long.toString(hours) + " hours," + Long.toString(minutes) +" Minutes," + Long.toString(seconds) + " seconds" ;
    }
}