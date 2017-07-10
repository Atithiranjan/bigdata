package com.aadhar.p1reducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class p1reducer extends Reducer<Text,LongWritable,Text,LongWritable>
{
public void reduce(Text key,Iterable<LongWritable> values,Context context) 
throws IOException, InterruptedException
{
 String fkey = key.toString();
 String flag = fkey.substring(2,fkey.length());
 long sum =0;
for(LongWritable iw:values) 
{
 sum += iw.get();
}
context.write(new Text(flag),new LongWritable(sum));
}
}

