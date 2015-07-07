/*
 * Don K Dennis (metastableB)
 * 07 July 2015
 * donkdennis [at] gmail.com
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

public class ParserDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job unweightingJob = new Job(conf);
        String input, output;
        String JobName = new String(args[2]+ "_Unweighting_");
        unweightingJob.setJarByClass(ParserDriver.class);
        unweightingJob.setJobName(JobName);
        input = args[0];
        output = args[1]+"/uw";
        FileInputFormat.setInputPaths(unweightingJob, new Path(input));
        FileOutputFormat.setOutputPath(unweightingJob, new Path(output));
        unweightingJob.setMapperClass(UnweightingMapper.class);
        unweightingJob.setReducerClass(UnweightingReducer.class);
        unweightingJob.setNumReduceTasks(1);
        unweightingJob.setOutputKeyClass(Text.class);
        unweightingJob.setOutputValueClass(Text.class);
     
        unweightingJob.waitForCompletion(true);

        Job listCreator = new Job(conf);
        String JobName2 = new String(args[2]+ "_ListCreator_");
        listCreator.setJarByClass(ParserDriver.class);
        listCreator.setJobName(JobName);
        input = args[1]+"/uw";
        output = args[1]+"/out";
        FileInputFormat.setInputPaths(listCreator, new Path(input));
        FileOutputFormat.setOutputPath(listCreator, new Path(output));
        listCreator.setMapperClass(ListCreatorMapper.class);
        listCreator.setReducerClass(ListCreatorReducer.class);
        listCreator.setNumReduceTasks(1);
        listCreator.setOutputKeyClass(Text.class);
        listCreator.setOutputValueClass(Text.class);
 
        listCreator.waitForCompletion(true); 
        
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3){
            System.err.println("Usage: <in> <output name>  <jobName>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new ParserDriver(), args);
        System.exit(res);
    }
}