/*
 * Don K Dennis (metastableB)
 * 10 July 2015
 * donkdennis [at] gmail.com
 *
 * I/O: source<tab>distance,start_point|color|adjacency list (csv)
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

public class st_Triple_PEPJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        int iterationCount = 0; 
        String s,t,u;
        s = args[3];
        t = args[4];
        u = args[5];

        long pathLength = 0, ut_path = 0, su_path = 0 ,st_path = 0;
        long noOfGrayNodes = 3 , runningTime, startTime, endTime, totalRunningTime = 0;
        boolean evenPathFound = false;
        boolean pathNotSet = true, su_notSet = true, ut_notSet = true;
        boolean connected = false, st_connected = false,su_connected = false, ut_connected = false;
        boolean st_even = false, su_even = false, ut_even = false;

        while( noOfGrayNodes > 0 && !connected){
            Configuration conf = new Configuration();
            conf.set("s",s);
            conf.set("t",t);
            conf.set("u",u);
            if(iterationCount == 0)
                conf.set("iterationCount","0");
            else
                conf.set("iterationCount","1");
            Job st_Triple_PEPJob = new Job(conf);
            String input, output;
            String JobName = new String(args[2]+ "_st_Triple_PEP_"+ String.valueOf(iterationCount));
            st_Triple_PEPJob.setJarByClass(st_Triple_PEPJob.class);
            st_Triple_PEPJob.setJobName(JobName);
            if(iterationCount == 0){
               input = args[0];
               output = args[1]+"/run" + String.valueOf(iterationCount);
            } else {
                input = args[1]+"/run" + String.valueOf(iterationCount-1);
                output = args[1]+"/run" + String.valueOf(iterationCount);
            }
            FileInputFormat.setInputPaths(st_Triple_PEPJob, new Path(input));
            FileOutputFormat.setOutputPath(st_Triple_PEPJob, new Path(output));
            
            st_Triple_PEPJob.setMapperClass(st_Triple_PEPMapper.class);
            st_Triple_PEPJob.setReducerClass(st_Triple_PEPReducer.class);
            st_Triple_PEPJob.setNumReduceTasks(1);

            st_Triple_PEPJob.setOutputKeyClass(Text.class);
            st_Triple_PEPJob.setOutputValueClass(Text.class);
         
            startTime = System.nanoTime();
            st_Triple_PEPJob.waitForCompletion(true); 
            endTime = System.nanoTime();
            runningTime = endTime - startTime;
            totalRunningTime += runningTime;

            iterationCount++;

            System.out.println("=====================================================================");
            System.out.println ("Job Running Time = " + runningTime);
            noOfGrayNodes =  st_Triple_PEPJob.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
            System.out.println("Number of Gray " + noOfGrayNodes);
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.stMeet).getValue() > 0)
                st_connected = true;
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.utMeet).getValue() > 0)
                ut_connected = true;
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.suMeet).getValue() > 0)
                su_connected = true;
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.stEvenPath).getValue() > 0)
                st_even = true;
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.utEvenPath).getValue() > 0)
                ut_even = true;
            if(st_Triple_PEPJob.getCounters().findCounter(MoreIterations.suEvenPath).getValue() > 0)
                su_even = true;
            System.out.println("=====================================================================");

            if(st_connected ) {
                connected = true;
                if(st_even)
                    st_path = (iterationCount)*2;
                else st_path = (iterationCount+1)*2 - 1;
                st_path = st_path - 1;
                pathLength = st_path;
            } else if(ut_connected && ut_notSet && !st_connected) {
                ut_notSet = false;      
                if(ut_even)
                    ut_path = (iterationCount)*2;
                else ut_path = (iterationCount+1)*2 - 1;
                ut_path = ut_path - 1;
            } else if (su_connected && su_notSet && !st_connected) {
                su_notSet = false;      
                if(su_even)
                    su_path = (iterationCount)*2;
                else su_path = (iterationCount+1)*2 - 1;
                su_path = su_path - 1;
            }
            if(!su_notSet && !ut_notSet && !st_connected) {
                pathLength = su_path + ut_path;
                if(pathLength <= (iterationCount)*2 - 1)
                    connected = true;
            }
            System.out.println("st_connected " + st_connected+" st_even "+st_even + " st_path "+st_path) ;
            System.out.println("su_connected " + su_connected+" su_even "+su_even + " su_path "+su_path) ;
            System.out.println("ut_connected " + ut_connected+" ut_even "+ut_even + " ut_path "+ut_path) ;
            System.out.println("path " + pathLength);
        }
        
        if(!connected)
            pathLength = -1;
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("Path Length "+ pathLength);
        System.out.println("=====================================================================");
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 6){
            System.err.println("Usage: <in> <output name>  <jobName> <s> <t> <u>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new st_Triple_PEPJob(), args);
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
