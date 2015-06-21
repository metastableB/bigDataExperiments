 /*
 * Don K Dennis (metastableB)
 * 19 June 2015
 * donkdennis [at] gmail.com
 *
 * Finding the SSSP using Dijkstra 
 * Inp: source<tab>distance|color|parent|adjacency list with wights (csv)
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


public class DijkstraJob extends Configured implements Tool {
	
	@Override
    public int run(String[] args) throws Exception {
        long startTime, endTime, totalRunningTime = 0;
        long iterationCount = 0;
        long noOfGray = 1;     
                
        startTime = System.nanoTime();
	    while (noOfGray > 0) {
	    	if(iterationCount != 0)
	    		source = extractMinimum(iterationCount,args); 
	    	mr_Relax(source, iterationCount,args);
	    	noOfGray +=  DijkstraJob.getCounters().findCounter(MoreIterations.numberOfIterations).getValue();
	    	noOfGray -= 1;
	    	iterationCount++;
	    }
	    endTime = System.nanoTime();
        runningTime = endTime - startTime;
        System.out.println("=====================================================================");
        System.out.println("Total Running Time "+ getRunningTime(runningTime));
        System.out.println("=====================================================================");
        return 0;
    }
    public void mr_Relax(String source, long iterationCount, String[] args) {
    	Configuration conf = new Configuration();
        conf.set("source", source);		
        Job DijkstraJob = new Job(conf);
        String input, output;
        String DijkstraJobName = new String(args[2]+ "_Dijkstra_"+ String.valueOf(iterationCount));
        DijkstraJob.setJarByClass(getClass());
        DijkstraJob.setJobName(DijkstraJobName);
        if(iterationCount == 0){
           input = args[0];
           output = args[1]+"/run" + String.valueOf(iterationCount);
        } else {
            input = args[1]+"/run" + String.valueOf(iterationCount-1);
            output = args[1]+"/run" + String.valueOf(iterationCount);
        }
        FileInputFormat.setInputPaths(DijkstraJob, new Path(input));
        FileOutputFormat.setOutputPath(DijkstraJob, new Path(output));
        
        DijkstraJob.setMapperClass(SSSP_Dijkstra_RealaxMapper.class);
 	   	DijkstraJob.setReducerClass(SSSP_Dijkstra_RelaxReducer.class);
    	DijkstraJob.setNumReduceTasks(1);

    	DijkstraJob.setOutputKeyClass(Text.class);
    	DijkstraJob.setOutputValueClass(Text.class);
     
        DijkstraJob.waitForCompletion(true); 
    }

    public String extractMinimum(long iterationCount, String[] args) {
    	Configuration conf = new Configuration();
        Job extractMinJob = new Job(conf);
        String input, output;
        String extratJobName = new String(args[2]+ "_Dijkstra_extract_");
        extractMinJob.setJarByClass(getClass());
        extractMinJob.setJobName(extractJobName);
        input = args[1]+"/run" + String.valueOf(iterationCount-1);
        output = args[1]+"/temp";
        FileInputFormat.setInputPaths(extractMinJob, new Path(input));
        FileOutputFormat.setOutputPath(extractMinJob, new Path(output));
        
        extractMinJob.setMapperClass(SSSP_Dijkstra_ExtractMinMapper.class);
 	   	extractMinJob.setReducerClass(SSSP_Dijkstra_ExtractMinReducer.class);
 	   	// WARNING: DONOT CHANGE THIS
    	extractMinJob.setNumReduceTasks(1);
    	extractMinJob.setOutputKeyClass(Text.class);
    	extractMinJob.setOutputValueClass(Text.class);
        extractMinJob.waitForCompletion(true); 

        Path pt = new Path(output + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        String minimum;
        while ((line = br.readLine()) != null) {
            minimum = line.trim();
        }
        FileSystem del = FileSystem.get(getConf());
        del.delete(new Path(args[1]+"/temp"), true); 
        return minimum;
        
    }
	    /*
	        System.out.println("=====================================================================");
	        System.out.println ("Job Running Time = " + runningTime);
	        
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
           
            System.out.println("Writing Path to file");
            writePath(args[1]+"/path.txt", path ,noPath);        
        }*/
       

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

    public static void main(String[] args) throws Exception {

        if(args.length != 5 && args.length != 4){
            System.err.println("Usage: <in> <output name> <jobName> <source> [<destination>]");
            System.exit(1);
        }
        int res = ToolRunner.run(new SSSP_DijkstraJob(), args);
        System.exit(res);
    }
}