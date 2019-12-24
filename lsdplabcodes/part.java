import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
           
   public class part {
           
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       
       public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
    	   String line = value.toString();
           String str[]=line.split(",");
           context.write(new Text(str[3]),new IntWritable(Integer.parseInt(str[1])));
      }                      
    } 
    public static class dpart extends Partitioner<Text,IntWritable>
    {
  	  public int getPartition(Text key,IntWritable value,int nr)
  	  {
  		 if(value.get()<30000)
  			  return 0;
  		 if(value.get() < 50000)
  		 	  return 1;
  		  else
  			  return 2;
  	  } 
  	 }
    public static class Reduce extends Reducer<Text,IntWritable, Text, IntWritable> {
    	int sum = 0;
       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
    	   for (IntWritable val : values) {
               sum += 1;
               context.write(key,val);
           }
       }
    }
           
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
           
           @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Averageage_survived");
           job.setJarByClass(average_age.class);
       
           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(IntWritable.class);
        //  job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
           
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
       
       job.setPartitionerClass(dpart.class);
       job.setNumReduceTasks(3);
       
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
           
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
       job.waitForCompletion(true);
    }
           
  }