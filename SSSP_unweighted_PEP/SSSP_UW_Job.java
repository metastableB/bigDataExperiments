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


public class SSSP_UW_Job extends Configured implements Tool {
	@Override
    public int run(String[] args) throws Exception {
        long runningTime, startTime, endTime, totalRunningTime = 0;
        long noOfGray = 1;
        long iterationCount = 0;
        String input, output;
        String PEPJobName = new String(args[2]+ "_PEP_0");
        Configuration conf = new Configuration();
		conf.set("source", args[3]);
        
	    while (noOfGray > 0) {
	    		        
	        Job PEPJob = new Job(conf);
	        PEPJob.setJarByClass(getClass());
	        PEPJob.setJobName(PEPJobName);
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
        // SECOND JOB Here
        if(args.length == 5) {       
            String pathPrinterJobName = new String(args[2]+ "_pathPrinter");
            String source = args[4];
    	    List <String> path = new ArrayList<String>();	
            path.add(0,source);
            long rootReached = 0;
            long loopCount = 0;
            boolean noPath = false;
           
            while(rootReached == 0 ){
                Configuration conf2 = new Configuration();
                conf2.set("source", source);		
                Job pathPrinterJob = new Job(conf2);
                pathPrinterJob.setJarByClass(getClass());
                pathPrinterJob.setJobName(pathPrinterJobName);          
                
                input = args[1]+"/run" + String.valueOf(iterationCount-1);
                output = args[1]+"/temp/run" + String.valueOf(loopCount);
            
                FileInputFormat.setInputPaths(pathPrinterJob, new Path(input));
                FileOutputFormat.setOutputPath(pathPrinterJob, new Path(output));
                
                pathPrinterJob.setMapperClass(SSSP_UW_PathMapper.class);
                //pathPrinterJob.setCombinerClass(Combiner.class);
                pathPrinterJob.setReducerClass(SSSP_UW_PathReducer.class);
                // DONOT MODIFY
                pathPrinterJob.setNumReduceTasks(1);

                pathPrinterJob.setOutputKeyClass(Text.class);
                pathPrinterJob.setOutputValueClass(Text.class);
             
                startTime = System.nanoTime();
                pathPrinterJob.waitForCompletion(true); 
                endTime = System.nanoTime();
                runningTime = endTime - startTime;
                totalRunningTime += runningTime;

                System.out.println("=====================================================================");
                System.out.println ("Job Running Time = " + runningTime);
                rootReached =  pathPrinterJob.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
                System.out.println("Counter "  + rootReached);
                System.out.println("=====================================================================");
                loopCount++;

                // Getting new source
                Path pt = new Path(output + "/part-r-00000");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                noPath = true;
                while ((line = br.readLine()) != null) {
                    noPath = false;
                    source = line.trim();
                }
                if(source.equals("source"))
                    rootReached = 1;
                if(noPath) {
                    noPath = true;
                    break;
                }
                //System.out.println("Source and eval "+source +" 1" + source.equals("null") +" 2"+ (source == null));
                path.add(0,source);
            }
              
            System.out.println("Removing temporary files");
            FileSystem del = FileSystem.get(getConf());
            del.delete(new Path(args[1]+"/temp"), true); 
            System.out.println("Writing Path to file");
            writePath(args[1]+"/path.txt", path ,noPath);        
        }
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(totalRunningTime));
        System.out.println("=====================================================================");

        return 0;
    }


    public static void main(String[] args) throws Exception {

        if(args.length != 5 && args.length != 4){
            System.err.println("Usage: <in> <output name> <jobName> <source> [<destination>]");
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

    public static  void  writePath(String pathToFile, List<String> path, boolean noPath) {
        Configuration config = new Configuration(); 
        config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));    
        try {
            FileSystem fs = FileSystem.get(config); 
            Path filenamePath = new Path(pathToFile);  
            if (fs.exists(filenamePath))
                fs.delete(filenamePath, true);
    
            FSDataOutputStream fin = fs.create(filenamePath);
            if(noPath)
                fin.write("NO PATH".getBytes());
            else {
                int size = path.size();
                for(String s : path) {
                    fin.write(s.getBytes());
                    size--;
                    if(size != 0)
                        fin.write(" --> ".getBytes());  
                }
            }
            fin.close();
        } catch(Exception e){
            System.out.println("Exception writing to file");
        }
    }
}