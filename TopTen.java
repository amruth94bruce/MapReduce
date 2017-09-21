package BigData;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTen {
	public static class Mapx extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		String[] s1,s2;
		String s4;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\n");
			
			for(String item:line){
				s1=item.toString().split("::");
				s4=s1[3].substring(0, 1);
				word.set(s1[2]);
				context.write(word, new IntWritable(Integer.parseInt(s4)));
			}
		}
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		private Text word1 = new Text();// type of output key
		String[] s1,s2;
		String s4;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\n");
			
			for(String item:line){
				s1=item.toString().split("\t");
				//s4=s1[3].substring(0, 1);
				s4="top::"+s1[1].trim()+"::";
				word.set(s1[0].trim());
				context.write(word, new Text(s4));
			}
		}
		
	}
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		private Text word1 = new Text();// type of output key
		String[] s1,s2;
		String s4;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\n");
			
			for(String item:line){
				s1=item.toString().split("::");
				s4="bus::"+s1[1].trim()+"::"+s1[2].trim();
				word.set(s1[0].trim());
				context.write(word, new Text(s4));
			}
		}
		
	}
	
public static class Reducex extends Reducer<Text,IntWritable,Text,FloatWritable> {
		
		private Text word = new Text(); 
		TreeMap<Float,List<String>> tree= new TreeMap<Float,List<String>>();
		
		float avg1;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0,c=0; // initialize the sum for each keyword
			
			for(IntWritable itemCount : values){
				sum= sum + itemCount.get();
				++c;
			}
			avg1= (float)sum/c;
			ArrayList<String> xvalSetOne = new ArrayList<String>();
			List<String> xvalSetOne1 = new ArrayList<String>();
			
			xvalSetOne.add(key.toString());
			
			if(tree.containsKey(avg1))
				{
					
					xvalSetOne1= tree.get(avg1);
					xvalSetOne1.add(key.toString());
				}
					else
						tree.put(avg1, xvalSetOne);
			}
		
		 public void cleanup(Context context) throws IOException, InterruptedException {
			Text word1 = new Text();
			word.set("top 10 values in hash map after sorting");
		
			int n=11;
			for(Float f: tree.descendingKeySet())
			 {
				for(String s:tree.get(f))
				{
					n--;
					if(n>0)
					{
					word1.set(s);
					context.write( word1,new FloatWritable(f));
					}
				}
					
			 }
			    
		}
		}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		private Text word = new Text(); 
		
		String[] s1,s2;
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int sum = 0,c=0; // initialize the sum for each keyword
			String ssx="",ssxx="";
			StringBuffer ss=new StringBuffer();
			for(Text sel : values){
				ss.append(sel);
				s1=ss.toString().split("::");
			}
			if(ss.toString().contains("bus") && ss.toString().contains("top::"))
			{	
				if(s1[0].contains("top"))
				{
					ssx=s1[0].substring(0, s1[0].length()-3)+","+s1[3]+","+s1[4].substring(0, s1[4].length()-3)+","+s1[1];
					context.write(key, new Text(ssx));
				}
				else if(s1[0].contains("bus"))
				{
					ssxx=s1[0].substring(0, s1[0].length()-3)+","+s1[1]+","+s1[2].substring(0, s1[2].length()-3)+","+s1[5];
					context.write(key, new Text(ssxx));
					
				}
			}
			ArrayList<String> xvalSetOne = new ArrayList<String>();
			List<String> xvalSetOne1 = new ArrayList<String>();
			
			xvalSetOne.add(key.toString());
			
			}
		}
	
	// Driver program
	public static void main(String[] args) throws Exception {
		 final String OUTPUT_PATH = "/axs165631/intermediate_output";
		 
		Configuration con = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
		System.err.println("Usage: WordCount <in1> <inter-output> <in2> <out>");
		System.exit(2);
		}
		Job jobx = new Job(con, "WordCount");
		jobx.setJarByClass(WordCount.class);
		jobx.setMapperClass(Mapx.class);
		jobx.setReducerClass(Reducex.class);
		jobx.setMapOutputKeyClass(Text.class);
		jobx.setOutputKeyClass(Text.class);
		jobx.setMapOutputValueClass(IntWritable.class);
		jobx.setOutputValueClass(FloatWritable.class);
		TextInputFormat.addInputPath(jobx, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(jobx, new Path(otherArgs[1]));
		jobx.waitForCompletion(true);
		
	Configuration conf = new Configuration();
	//String[] otherArgs1 = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	//if (otherArgs.length != 3) {
	//System.err.println("Usage: WordCount <in1> <in2>  <out>");
//	System.exit(2);
//	}
	// create a job with name "wordcount"
	Job job = new Job(conf, "TopTen");
	job.setJarByClass(TopTen.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	// uncomment the following line to add the Combiner 
	// gf job.setCombinerClass(Reduce.class);
	// set output key type
	job.setMapOutputKeyClass(Text.class);
	job.setOutputKeyClass(Text.class);
	
	// set output value type
	job.setMapOutputValueClass(Text.class);
	job.setOutputValueClass(Text.class);
	//set the HDFS path of the input data
	//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// set the HDFS path for the output
	//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	//Wait till job completion

	 MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, Map.class);
	 MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, Map1.class);
	 Path outputPath = new Path(args[3]);
	  
	 FileOutputFormat.setOutputPath(job, outputPath);
	 outputPath.getFileSystem(conf).delete(outputPath);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}