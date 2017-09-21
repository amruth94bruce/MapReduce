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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class TopRated {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		private Text word1 = new Text();
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
	
	
	public static class Reduce extends Reducer<Text,IntWritable,FloatWritable,Text> {
		
		private Text word = new Text(); 
		TreeMap<Float,List<String>> tree= new TreeMap<Float,List<String>>();
		
		float avg1;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0,c=0; 
			
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
			int n=11;
			for(Float f: tree.descendingKeySet())
			 {
				for(String s:tree.get(f))
				{
					n--;
					if(n>0)
					{
					word1.set(s);
					context.write(new FloatWritable(f), word1);
					}
				}
					
			 }
			 
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
	Job job = new Job(conf, "WordCount");
	job.setJarByClass(TopRated.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	// uncomment the following line to add the Combiner 
	// gf job.setCombinerClass(Reduce.class);
	// set output key type
	job.setMapOutputKeyClass(Text.class);
	job.setOutputKeyClass(FloatWritable.class);
	
	// set output value type
	job.setMapOutputValueClass(IntWritable.class);
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