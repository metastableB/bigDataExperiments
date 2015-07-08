/*
 * Don K Dennis (metastableB)
 * 09 July 2015
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

public class DivideConquerMaxJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
    	long iterationCount = 0, terminationValue = Integer.valueOf(args[3]);
    	String k = args[3];
    	long noOfGrayNodes = 2 , runningTime, startTime, endTime, totalRunningTime = 0;
    	
    	while(terminationValue > 1) {
    		if(terminationValue > 10)
	        	terminationValue = terminationValue / 10;
	        else 
	        	terminationValue = 1;
    		System.out.println("termi 1" + terminationValue);
	    	Configuration conf = new Configuration();
	    	conf.set("iterationCount","0");
	    	if(iterationCount != 0) 
	    		conf.set("iterationCount","1");
	    	
	    	k = String.valueOf(terminationValue);

	        conf.set("k",k);
	        Job divideConquerMaxJob = new Job(conf);
	        String input, output;
	        String JobName = new String(args[2]+ "_DCMax_");
	        divideConquerMaxJob.setJarByClass(DivideConquerMaxJob.class);
	        divideConquerMaxJob.setJobName(JobName);
	        if(iterationCount == 0) {
	        	input = args[0];
	        	output = args[1]+"/run"+String.valueOf(iterationCount);
	        } else {
	        	input = args[1]+"/run"+String.valueOf(iterationCount - 1);
	        	output = args[1]+"/run"+String.valueOf(iterationCount);
	        }
	        
	        FileInputFormat.setInputPaths(divideConquerMaxJob, new Path(input));
	        FileOutputFormat.setOutputPath(divideConquerMaxJob, new Path(output));
	        divideConquerMaxJob.setMapperClass(DivideConquerMaxMapper.class);
	        divideConquerMaxJob.setReducerClass(DivideConquerMaxReducer.class);
	        divideConquerMaxJob.setNumReduceTasks(10);
			if(k.equals("1"))
				divideConquerMaxJob.setNumReduceTasks(1);
	        divideConquerMaxJob.setOutputKeyClass(Text.class);
	        divideConquerMaxJob.setOutputValueClass(Text.class);
	     	
	     	startTime = System.nanoTime();
            divideConquerMaxJob.waitForCompletion(true);
            endTime = System.nanoTime();
            runningTime = endTime - startTime;
            totalRunningTime += runningTime;
	        
	        iterationCount++;
	        terminationValue =  divideConquerMaxJob.getCounters().findCounter(NumberOfLines.numberOfLines).getValue(); 
	        
	        System.out.println("termi 2" + terminationValue);

            System.out.println("=====================================================================");
            System.out.println ("Job Running Time = " + runningTime);
            System.out.println("=====================================================================");
        }
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("=====================================================================");
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 4 || Integer.valueOf(args[3]) <= 0){
            System.err.println("Usage: <in> <output>  <jobName> <~numberOfLines>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new DivideConquerMaxJob(), args);
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
