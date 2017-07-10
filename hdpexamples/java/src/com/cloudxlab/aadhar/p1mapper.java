package com.cloudxlab.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class p1mapper extends Mapper<Object,Text,Text,LongWritable>
{
public void map(Object key, Text value, Context context) throws IOException, InterruptedException
 {
      String[] words = value.toString().split("\t");
      
      statecity = words[0] + words[1]
      
      int apr = Integer.parseInt(words[2]);
      int rej = Integer.parseInt(words[3]);
       
      
      
      
 

 }
}
