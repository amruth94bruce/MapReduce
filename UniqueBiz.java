package BigData;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UniqueBiz {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		private Text word1 = new Text();// type of output key
		String[] s1,s2;
		String s4;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\n");
			
			for(String item:line){
				
				s1=item.toString().split("::");
				
				if(s1[1].contains("Palo Alto"))
				{
					s4=s1[2].substring(5,s1[2].length()-1);
					s2=s4.split(",");
					word.set("Palo Alto");
					for(String s:s2)
					{
						word1.set(s.trim());
						context.write(word, word1);
					}
				}
			}
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		private Text result = new Text();
		//private Text word = new Text(); 
		String translations = "";
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
			Set<Text> s=new HashSet<Text>();
			for(Text itemCount:values){
				if(!s.contains(itemCount))
				{
					s.add(itemCount);
					translations += "|" + itemCount.toString();
				}
			
			result.set(translations.substring(6));
			context.write(key,result);
		
		}
	}
	
	
	// Driver program
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if (otherArgs.length != 2) {
	System.err.println("Usage: WordCount <in> <out>");
	System.exit(2);
	}
	// create a job with name "wordcount"
	Job job = new Job(conf, "UniqueBiz");
	job.setJarByClass(UniqueBiz.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	// uncomment the following line to add the Combiner 
	job.setCombinerClass(Reduce.class);
	// set output key type
	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(Text.class);
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	 Path outputPath = new Path(args[1]);
	  
	 FileOutputFormat.setOutputPath(job, outputPath);
	 outputPath.getFileSystem(conf).delete(outputPath);
	//Wait till job completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}