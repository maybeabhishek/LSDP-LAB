
import java.io.IOException;
//import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;





public class nline {
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{			  
			context.write(key,value);	
		} 
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 3);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		
		Job job = new Job(conf, "customer2");
		
		job.setInputFormatClass(NLineInputFormat.class);
    job.setNumReduceTasks(0);
		
		job.setJarByClass(nline.class);
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
	}	
	
}
