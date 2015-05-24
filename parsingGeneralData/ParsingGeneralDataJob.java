/*
 * Don K Dennis (metastableB)
 * 23 May 2015
 * donkdennis [at] gmail.com
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * (c) IIIT Delhi , 2015
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
import java.util.Scanner;

public class ParsingGeneralDataJob extends BaseJob {
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
                return ParsingGeneralDataJob.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return GeneralMapper.class;
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
                return GeneralReducer.class;
            }
        };
       
        return setupJob(jobName, jobInfo);
    
    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {
        Job job;
        job = getJobConf(args, args[2]); 
        String input, output;
       
        input = args[0];
        output = args[1];

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true); 
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Scanner inp = new Scanner(System.in);
        if(args.length != 3){
            System.err.println("Usage: <in> <output name> <jobName>");
            System.exit(1);
        }
        System.out.println("Specified the source node have we?(1 yes)");
        int i = inp.nextInt();
        if (i!=1)
            System.exit(1);
        int res = ToolRunner.run(new Configuration(), new ParsingGeneralDataJob(), args);
        System.exit(res);
    }

}
