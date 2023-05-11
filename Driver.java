import java.io.IOException;
import java.io.BufferedReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.filecache.DistributedCache;

public class EngineDriver {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(EngineDriver.class);
		
		try {
			DistributedCache.addCacheFile(new URI(args[0]), job.getConfiguration());
		} catch(Exception e) {
			
		System.out.println(e);}
		job.setMapperClass(EngineMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(EngineReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[2]));
		job.waitForCompletion(true);
	
	}
}