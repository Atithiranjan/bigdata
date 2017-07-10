package com.cloudxlab.aadhar;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class p1mapper extends Mapper<Object,Text,Text,LongWritable>
{
public void map(Object key, Text value, Context context) throws IOException, InterruptedException
 {
     int apr;
     int rej;
 
      String[] words = value.toString().split("\t");
      for(int i=0;i<4;i++)
      {
        apr = Integer.parseInt(words[2]);
        rej = Integer.parseInt(words[3]);
        if(i==0)
         {
         String sa = "A1" + words[0];
         context.write(new Text(sa),new LongWritable(apr));
         }
         else if(i==1)
         {
         String sr = "A2" + words[0];
          context.write(new Text(sr),new LongWritable(rej));
         }
         else if(i==2)
         {
         String ca = "A3" + words[1];
          context.write(new Text(ca),new LongWritable(apr));
         }
         else 
         {
         String cr = "A4" + words[1];
          context.write(new Text(cr),new LongWritable(rej));
         }
      }   
  }
}
