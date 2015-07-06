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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class PEPJob extends BaseJob {
    // method to set the configuration for the job and the mapper and the reducer classes
    private Job getJobConf(String[] args, String jobName) 
        throws Exception {

    // Defining the abstract class objects
        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return PEPJob.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return PEPMapper.class;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return Text.class;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return Text.class;
            }

            @Override
            public Class<? extends Reducer> getReducerClass() {
                return PEPReducer.class;
            }
        };
       
        return setupJob(jobName, jobInfo);
    
    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {
        int iterationCount = 0; 
        Job job;
        // No of grey nodes
        long terminationValue = 1 , runningTime, startTime, endTime, totalRunningTime = 0;
        String jobName = new String(args[2]+"_pass_");
               
        while( terminationValue > 0 ){
            job = getJobConf(args , (jobName + (String.valueOf(iterationCount)))); 
            String input, output;
           
            // Setting the input file and output file for each iteration
            // During the first time the user-specified file will be the
            // input whereas for the subsequent iterations
            // the output of the previous iteration will be the input
            // NOTE: Please be clear of how the input output files are set
            //       before proceding.
            
            // for the first iteration the input will be the first input argument
            if (iterationCount == 0) 
                input = args[0];
            else
                // for the remaining iterations, the input will be the output of the previous iteration
                input = args[1] + iterationCount;
            output = args[1] + (iterationCount + 1);

            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            
            startTime = System.nanoTime();
            job.waitForCompletion(true); 
            endTime = System.nanoTime();
            runningTime = endTime - startTime;
            totalRunningTime += runningTime;

            System.out.println("=====================================================================");
            System.out.println ("Job Running Time = " + runningTime);
            terminationValue =  job.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
            System.out.println("Counter "+terminationValue);
            System.out.println("=====================================================================");
            // if the counter's value is incremented in the reducer(s), then there are more
            // GRAY nodes to process implying that the iteration has to be continued.
            iterationCount++;
        }
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("=====================================================================");
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3){
            System.err.println("Usage: <in> <output name> <jobName>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new PEPJob(), args);
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