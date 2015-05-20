/*
 * Don K Dennis (metastableB)
 * 8 May 2015
 * donkdennis [at] gmail.com
 *
 * Code part of an implementation of BFS using mapReduce Framerwork.
 * This code is inspired by the article here at :
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *
 * This is a generic job creator method which can be used to set up the job easily.
 * To use this method first define a getJobConf() method which will in turn extend the 
 * jobInfo object and setup a job using setupJob() as required. The job objet should be
 * returned and processed.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import java.io.IOException;
 
public abstract class BaseJob extends Configured implements Tool {
 
        protected Job setupJob(String jobName,JobInfo jobInfo) throws Exception {

        Job job = new Job(new Configuration(), jobName);
        job.setJarByClass(jobInfo.getJarByClass());
     
        job.setMapperClass(jobInfo.getMapperClass());
        if (jobInfo.getCombinerClass() != null)
            job.setCombinerClass(jobInfo.getCombinerClass());
        job.setReducerClass(jobInfo.getReducerClass());
       
        // TODO : set number of reducers as required
        job.setNumReduceTasks(6);
        
        job.setOutputKeyClass(jobInfo.getOutputKeyClass());
        job.setOutputValueClass(jobInfo.getOutputValueClass());
        return job;
    }
   
   // Implement an abstract class for JobInfo object
    protected abstract class JobInfo {
        public abstract Class<?> getJarByClass();
        public abstract Class<? extends Mapper> getMapperClass();
        public abstract Class<? extends Reducer> getCombinerClass();
        public abstract Class<? extends Reducer> getReducerClass();
        public abstract Class<?> getOutputKeyClass();
        public abstract Class<?> getOutputValueClass();
       
    }
}

