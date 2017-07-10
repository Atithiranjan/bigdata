package com.cloudxlab.aadhar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.cloudxlab.customreader.NLinesInputFormat;

public class p1driver {

	public static void main(String[] args) throws Exception {
  
  JobConf conf = new JobConf();
  Job job = new Job(conf,"Aadhar Counting");
  
  setJarByClass(p1driver.class);
  setMapperClass(p1mapper.class);
  //setReducerClass(p1reducer.class);
  
  
  setOutputKeyClass(Text.class);
  setOutputValueClass(LongWritable.class);
  
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job,new Path(args[1]));
  
  boolean result = job.waitForCompletion(true);
  System.exit(result ? 0 : 1);
  }
  }
