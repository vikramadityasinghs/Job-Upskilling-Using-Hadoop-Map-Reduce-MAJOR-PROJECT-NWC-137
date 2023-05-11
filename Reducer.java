import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.LinkedHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EngineReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
		Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HashMap<String,Double> courseList=new HashMap<String,Double>();
		String count="";
		for(Text input : values)
		{
			String[] arr=input.toString().split(";");
			courseList.put(arr[0],Double.parseDouble(arr[1]));
		}
		
		HashMap<String,Double> reverseSortedMap=new LinkedHashMap<>();
		courseList.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(20).forEachOrdered(x->reverseSortedMap.put(x.getKey(),x.getValue()));
		
		Iterator it = reverseSortedMap.entrySet().iterator();
		String s="";
		
		   while (it.hasNext())
		   {
			   String tmp="";
		       Map.Entry pair = (Map.Entry)it.next();
		       String course=pair.getKey().toString();
		       double similarity=(Double)pair.getValue();
		       tmp=tmp+course+":"+Double.toString(similarity);
		      s=s+";"+tmp;
		       
		   }
			
		
		if (key.toString()!=null && !key.toString().trim().isEmpty() )
		{
			context.write(key, new Text(s));
		}
	}
	

}