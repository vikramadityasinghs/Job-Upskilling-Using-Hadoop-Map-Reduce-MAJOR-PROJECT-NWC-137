import org.apache.hadoop.mapreduce.Mapper;

import java.lang.Math;
import java.util.StringTokenizer;
import java.io.IOException;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
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
import org.apache.hadoop.filecache.DistributedCache;


public class EngineMapper extends Mapper<LongWritable,Text,Text,Text>{

	HashMap<String,Double> courseRatingLookup=new HashMap<String,Double>();
	HashMap<String,HashMap<String,Double>> courseTFLookup=new HashMap<String,HashMap<String,Double>>();
	HashMap<String,List<String>> courseWordbagLookup=new HashMap<String,List<String>>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
	throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		Path[] files= DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		for(Path p : files)
		{
		
			if(p.getName().toString().equals("Course.txt"))
			{
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line=reader.readLine();
			
				do
				{
					List<String> courseNames=new ArrayList<>();
					HashMap<String,Double> courseTF=new HashMap<String,Double>();
					String[] tokens=line.split("\\|");
					tokens[2]=tokens[2].trim().substring(1,tokens[2].length()-1);
					String[] names=tokens[2].trim().split(",");
					
					for(int i=0;i<names.length;i++)
					{
						names[i]=names[i].trim();
						courseNames.add(names[i].substring(1,names[i].length()-1));
					}
					
					tokens[3]=tokens[3].trim().substring(1,tokens[3].length()-1);
					String[] nameTF=tokens[3].trim().split(",");
					
					for(int i=0;i<nameTF.length;i++)
					{
						nameTF[i]=nameTF[i].trim();
						String[] dictSplit=nameTF[i].split(":");
						dictSplit[0]=dictSplit[0].trim();
						String a=dictSplit[0].substring(1,dictSplit[0].length()-1);
						dictSplit[1]=dictSplit[1].trim();
						double b=Double.parseDouble(dictSplit[1]);
						courseTF.put(a,b);
					}
					courseRatingLookup.put(tokens[0].trim(),Double.parseDouble(tokens[1]));
					courseTFLookup.put(tokens[0].trim(),courseTF);
					courseWordbagLookup.put(tokens[0].trim(),courseNames);
					line= reader.readLine();
					
				}while(line!=null);
			}
			if(courseRatingLookup.isEmpty()){
					throw new IOException("Unable to find the course lookup in distributed cache");
			}
	
		}

	}



@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
throws IOException, InterruptedException {
// TODO Auto-generated method stub


	List<String> jobNames=new ArrayList<>();
	HashMap<String,Double> jobTF=new HashMap<String,Double>();
	
	HashMap<String,HashMap<String,Double>> jobTFLookup=new HashMap<String,HashMap<String,Double>>();
	HashMap<String,List<String>> jobWordbagLookup=new HashMap<String,List<String>>();
	
	
	
	String inputText = value.toString();
	String[] tokens = inputText.split("\\|");
	tokens[1]=tokens[1].trim().substring(1,tokens[1].length()-1);
	String[] names=tokens[1].trim().split(",");
	
	for(int i=0;i<names.length;i++)
	{
		names[i]=names[i].trim();
		jobNames.add(names[i].substring(1,names[i].length()-1));
	}
	
	tokens[2]=tokens[2].trim().substring(1,tokens[2].length()-1);
	String[] nameTF=tokens[2].trim().split(",");
	
	for(int i=0;i<nameTF.length;i++)
	{
		nameTF[i]=nameTF[i].trim();
		String[] dictSplit=nameTF[i].split(":");
		dictSplit[0]=dictSplit[0].trim();
		String a=dictSplit[0].substring(1,dictSplit[0].length()-1);
		dictSplit[1]=dictSplit[1].trim();
		double b=Double.parseDouble(dictSplit[1]);
		jobTF.put(a,b);
	}
	
	jobTFLookup.put(tokens[0].trim(),jobTF);
	jobWordbagLookup.put(tokens[0].trim(),jobNames);
	
	String jobTitle=tokens[0].trim();
	Iterator it = courseWordbagLookup.entrySet().iterator();
	   while (it.hasNext())
	   {
	       Map.Entry pair = (Map.Entry)it.next();
	       String cname=pair.getKey().toString();
	       List<String> cNamesList=new ArrayList<>();
	       cNamesList=(ArrayList<String>)pair.getValue();
	       double sim=0;
	       
	       double csqr=0;
	       double jsqr=0;
	       double cosineSimilarity=0;
	       double finalSimilarity=0;
	       for(int i=0;i<jobNames.size();i++)
	       {
	       
		        if(cNamesList.contains(jobNames.get(i))) {
		        sim=sim+(jobTF.get(jobNames.get(i))*courseTFLookup.get(cname).get(jobNames.get(i)));
		
	        }
	        jsqr=jsqr+(jobTF.get(jobNames.get(i))*jobTF.get(jobNames.get(i)));
	       
	       
	       }
	       
	       for(int j=0;j<courseWordbagLookup.get(cname).size();j++) {
	        csqr=csqr+((courseTFLookup.get(cname).get(courseWordbagLookup.get(cname).get(j))) * (courseTFLookup.get(cname).get(courseWordbagLookup.get(cname).get(j))));
	       }
	       
	       if(jsqr==0 || csqr==0) {
		        cosineSimilarity=0;
		        finalSimilarity=0;
		        context.write(new Text(jobTitle), new Text(cname+";"+Double.toString(finalSimilarity)));
	       }
	       
	       else {
		       cosineSimilarity=(double)sim/((Math.sqrt(jsqr))*(Math.sqrt(csqr)));
		       finalSimilarity=cosineSimilarity * courseRatingLookup.get(cname);
		       context.write(new Text(jobTitle), new Text(cname+";"+Double.toString(finalSimilarity)));
	       }
	  
	   }

	
	}

}