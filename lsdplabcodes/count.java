
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class count {
	public enum ct {
		cnt, nt
	};

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] str = line.split(",");
			int g1 = Integer.parseInt(str[8]);
			float avg = 0;
			for (int i = 8; i <= 10; i++)
				avg += Integer.parseInt(str[i]);
			avg /= 3;
			if (g1 < 10) {
				context.getCounter(ct.cnt).increment(1);
				context.write(new Text(str[0] + " " + g1), null);
			}
			if (avg > 13) {
				context.getCounter(ct.nt).increment(1);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Count");
		job.setJarByClass(count.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
		Counters cn = job.getCounters();
		cn.findCounter(ct.cnt).getValue();
		cn.findCounter(ct.nt).getValue();
	}

}