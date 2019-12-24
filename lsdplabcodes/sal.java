import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
           
   public class sal {
           
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
       
       private IntWritable exp = new IntWritable();
       private IntWritable sal = new IntWritable();
       private Text country = new Text();
       public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");
           
           if(Float.parseFloat(str[0])>15 && Float.parseFloat(str[1])>60000){
               context.write(new LongWritable(),new Text(str[0]+" "+str[1]+" "+str[3]));
               }
      }       
               
    } 
    static int sum = 0;
    public static class Reduce extends Reducer<LongWritable, Text, Text,IntWritable> {
   
       public void reduce(LongWritable key, Iterable<Text> values, Context context) 
         throws IOException, InterruptedException {
           
           for (Text val : values) {
               sum += 1;
               context.write(new Text(val),null);
           }        
       }
       public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("The total number of records are: "),new IntWritable(sum));
		} 
    }
    
           
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
           
           @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Averageage_survived");
           job.setJarByClass(sal.class);
       
           job.setMapOutputKeyClass(LongWritable.class);
           job.setMapOutputValueClass(Text.class);
        //  job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
           
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
           
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
           
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
       job.waitForCompletion(true);
    }
           
  }