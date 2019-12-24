import java.io.IOException;
import java.util.*;

import javax.xml.soap.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class try{
    public static class map extends Mapper<LongWritable, Text, Text, IntWritable>{
        IntWritable one = new IntWritable(1);
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line[] = value.toString().split(",");
            for(String i: line){
                context.write(new Text(i), one);
            }
        }
    }

    public static class reduce extends Reducer<Text,IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWriter> values, Context context) throws IOException, InterruptedException{
            int sum= 0; 
            for(IntWritable val:values)
                sum+=val;
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String args[]) {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(try.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
    }
}