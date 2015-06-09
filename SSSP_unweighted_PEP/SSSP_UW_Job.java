 /*
 * Don K Dennis (metastableB)
 * 09 June 2015
 * donkdennis [at] gmail.com
 *
 * Finding the SSSP for a unweighed graph using 
 * the Partial Edges Passing BFS (PEP) algorithm,
 * As described in
 * Implementing Quasi-Parallel BFS in MapReduce for Large Scale Social Network Mining
 * - Lianghong Qian, Lei Fan and Jianhua Li
 *
 * Inp: source<tab>distance|color|parent|adjacency list (csv)
 * Out: source<tab>distance|color|parent|<adj list>
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

public class SSSP_UW_Job extends Configured implements Tool {
	@Override
    public int run(String[] args) throws Exception {
        long runningTime, startTime, endTime, totalRunningTime = 0;
        long noOfGray = 1;
        long iterationCount = 0;
        String PEPJobName = new String(args[2]+ "_PEP_0");
        Configuration conf = new Configuration();
		conf.set("source", args[3]);
        
	    while (noOfGray > 0) {
	    		        
	        Job PEPJob = new Job(conf);
	        PEPJob.setJarByClass(getClass());
	        PEPJob.setJobName(PEPJobName);
	        String input, output;
            if(iterationCount == 0){
	           input = args[0];
	           output = args[1]+"/run" + String.valueOf(iterationCount);
            } else {
                input = args[1]+"/run" + String.valueOf(iterationCount-1);
                output = args[1]+"/run" + String.valueOf(iterationCount);
            }
	        FileInputFormat.setInputPaths(PEPJob, new Path(input));
	        FileOutputFormat.setOutputPath(PEPJob, new Path(output));
	        
	        PEPJob.setMapperClass(SSSP_UW_PEPMapper.class);
	 	   	//PEPJob.setCombinerClass(Combiner.class);
	    	PEPJob.setReducerClass(SSSP_UW_PEPReducer.class);
	    	PEPJob.setNumReduceTasks(1);

	    	PEPJob.setOutputKeyClass(Text.class);
	    	PEPJob.setOutputValueClass(Text.class);
	     
	        startTime = System.nanoTime();
	        PEPJob.waitForCompletion(true); 
	        endTime = System.nanoTime();
	        runningTime = endTime - startTime;
	        totalRunningTime += runningTime;

	        System.out.println("=====================================================================");
	        System.out.println ("Job Running Time = " + runningTime);
	        noOfGray =  PEPJob.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
	        System.out.println("Counter "  + noOfGray);
	        System.out.println("=====================================================================");
            iterationCount++;
	   }
        /* SECOND JOB Here
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
        //terminationValue =  edgeCreatorJob.getCounters().findCounter(MoreIterations.numberOfIterations).setValue();*/
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("=====================================================================");
        return 0;
    }


    public static void main(String[] args) throws Exception {

        if(args.length != 5){
            System.err.println("Usage: <in> <output name> <jobName> <source> <destination>");
            System.exit(1);
        }
        int res = ToolRunner.run(new SSSP_UW_Job(), args);
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