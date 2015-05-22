/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis@gmail.com
 *
 * Code part of an implementation of BFS using mapReduce Framerwork.
 * This code is inspired by the article here at :
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *
 * This is a generic job creator method which can be used to set up the job easily.
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

public class SSSPJob extends BaseJob {
    // method to set the configuration for the job and the mapper and the reducer classes
    private Job getJobConf(String[] args) 
        throws Exception {

    // Defining the abstract class objects
        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return SSSPJob.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return SearchMapper.class;
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
                return SearchReducer.class;
            }
        };
       
        return setupJob("ssspjob", jobInfo);
    
    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {
        int iterationCount = 0; 
        Job job;
        // No of grey nodes 
        long terminationValue = 1;
               
        while( terminationValue > 0 ){
            job = getJobConf(args); 
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

            job.waitForCompletion(true); 

            terminationValue =  job.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
            // if the counter's value is incremented in the reducer(s), then there are more
            // GRAY nodes to process implying that the iteration has to be continued.
            iterationCount++;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new SSSPJob(), args);
        if(args.length != 2){
            System.err.println("Usage: <in> <output name> ");
            System.exit(1);
        }
        System.exit(res);
    }

}