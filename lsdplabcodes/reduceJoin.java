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
public class reduceJoin {
	public static class custmapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
	 String line[] = value.toString().split(",");
	 context.write(new Text(line[0]), new Text("cust"+","+line[1]));
	  }
	 }

	 public static class transmapper extends Mapper<LongWritable,Text,Text,Text>
	 {
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException{
		  String line[] = value.toString().split(",");
		  context.write(new Text(line[0]), new Text("trans"+","+line[1]+"," +line[2]));
	  }
	 }

	 public static class jreducer extends Reducer<Text,Text,Text,Text>
	 {
	  String st1;
	  
	  public void reduce(Text key, Iterable<Text> values, Context context ) 
	  throws IOException, InterruptedException
	  {
		  int c=0,amt=0; 
	  for(Text val:values)
	   {
		   
		   String[] line = val.toString().split(",");
		   
		   if (line[0].equals("trans")) 
		   {
			   if(line[2].equals("card")){
			   c++;
			amt += Float.parseFloat(line[1]);
			   }
		   
		   } 
		   else if (line[0].equals("cust")) 
		   {
			   st1=  line[1];
			   
		   		   }
	   }
		if(amt!=0)
			context.write(new Text(st1), new Text(c+","+amt));
		   }
		   }
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = new Job(conf, "Reduce-side join");
		 job.setJarByClass(reduceJoin.class);
		 job.setReducerClass(jreducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		  
		 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, custmapper.class);
		 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, transmapper.class);
		 Path outputPath = new Path(args[2]);
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 outputPath.getFileSystem(conf).delete(outputPath);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
	

}
