/*
 * Don K Dennis (metastableB)
 * 07 July 2015
 * donkdennis [at] gmail.com
 *
 * I/O: u<tab>v
 *
 * (c) IIIT Delhi, 2015
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class UnweightingMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] nodes = value.toString().split("\t");
        context.write(new Text(nodes[0]), new Text(nodes[1]));
    }
}



