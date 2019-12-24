import java.io.*;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class topk {

	public static class Map extends Mapper<LongWritable, Text,NullWritable,Text> 
	{
	private TreeMap<Integer, Text> salary = new TreeMap<Integer, Text>();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		 String line = value.toString();
         String str[]=line.split(",");
         int i = Integer.parseInt(str[2]);
	        salary.put(i,new Text(value));
	         if (salary.size() >= 10) {
	         salary.remove(salary.firstKey());
	        }      
	}
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{   	       
		for ( Text name : salary.values() ) {
	       context.write(NullWritable.get(), name);
	    }
	 }
	}


	 public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> 
	{ 	    
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	TreeMap<Integer, Text> salary = new TreeMap< Integer, Text>();
	        for (Text value : values) {
		 	        String line = value.toString();
		  	          String[] elements=line.split(",");
		  	          
		  	         int  i= Integer.parseInt(elements[2]);
		  	       salary.put(i, new Text(value));
		    	            if (salary.size() >= 10) {
		  	      salary.remove(salary.firstKey());
		  	  	            }
		   	        }
		  	        for (Text t : salary.values()) {
		        context.write(NullWritable.get(), t);
		   	        }
		  	    }
		 	  	}

	 public static void main(String[] args) throws Exception {
	       Configuration conf = new Configuration();
	           
	           @SuppressWarnings("deprecation")
	        Job job = new Job(conf, "Averageage_survived");
	           job.setJarByClass(topk.class);
	       
	           job.setMapOutputKeyClass(NullWritable.class);
	           job.setMapOutputValueClass(Text.class);
	        //  job.setNumReduceTasks(0);
	           
	           job.setOutputKeyClass(NullWritable.class);
	       	job.setOutputValueClass(Text.class);
	       	      job.setNumReduceTasks(1);
	           
	           
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
