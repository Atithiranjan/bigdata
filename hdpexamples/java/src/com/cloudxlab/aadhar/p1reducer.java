package com.aadhar.p1reducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class p1reducer extends Reducer<Text,LongWritable,Text,LongWritable>
{
void reduce(Text key,Iterable<LongWritable> values,Context context) 
throws IOException, InterruptedException
{
 String flag = key.substring(2,key.length());
 long sum =0;
for(LongWritable iw:values) 
{
 sum = sum + iw;
}
context.write(new Text(flag),new LongWritable(sum));
}
}

