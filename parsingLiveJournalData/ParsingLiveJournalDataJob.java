/*
 * Don K Dennis (metastableB)
 * 16 May 2015
 * donkdennis [at] gmail.com
 *
 * Parsing input from the SNAP facebook dataset
 * into files with values of the form 
 * node cdv list of neighbours
 *
 * Input of following form in each line indicating an edge
 * <node> <node>
 * http://snap.stanford.edu/data/egonets-Facebook.html
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

public class ParsingLiveJournalDataJob extends BaseJob {
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
                return ParsingLiveJournalDataJob.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return LiveJournalMapper.class;
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
                return LiveJournalReducer.class;
            }
        };
       
        return setupJob("LiveJournalParsingJob", jobInfo);
    
    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {
        Job job;
        job = getJobConf(args); 
        String input, output;
       
        input = args[0];
        output = args[1];

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true); 
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 2){
            System.err.println("Usage: <in> <output name> ");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new ParsingLiveJournalDataJob(), args);
        System.exit(res);
    }

}