import java.io.*;
import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class baseline {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			String line = value.toString();
	         String words[]=line.split(",");
			for (String s : words) {
				context.write(new Text(s), new Text(fileName));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap m = new HashMap();
			int count = 0;
			for (Text t : values) {
				String str = t.toString();
				if (m != null && m.get(str) != null) {
					count = (int) m.get(str);
					m.put(str, ++count);
				} else {
					m.put(str, 1);
				}

			}
			context.write(key, new Text(m.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
     Job job = new Job(conf, "Averageage_survived");
        job.setJarByClass(baseline.class);
    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
     //  job.setNumReduceTasks(0);
        
        job.setOutputKeyClass(Text.class);
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