package com.cloudxlab.sujimr;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class sujireducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
public void reduce(Text key, Iterable <LongWritable> values, Context context)
throws IOException, InterruptedException
{
Long sum = 0;
for(LongWritable iw:values)
{
 sum = sum + iw.get();
}

context.write(key, new LongWritable(sum));

}


}
