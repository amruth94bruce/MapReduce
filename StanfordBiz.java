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
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StanfordBiz {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		//private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
		private static HashSet<String> DepartmentMap1=new HashSet<String>();
		private BufferedReader fis;
		private String strrating = "";
		private Text txtMapOutputKey = new Text("");
		private Text txtMapOutputValue = new Text("");
		
		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws java.io.IOException,
				InterruptedException {

			try {

				Path[] stopWordFiles = new Path[0];
				stopWordFiles = context.getLocalCacheFiles();
				System.out.println(stopWordFiles.toString());
				if (stopWordFiles != null && stopWordFiles.length > 0) {
					for (Path stopWordFile : stopWordFiles) {
						readStopWordFile(stopWordFile);
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading stop word file: " + e);

			}

		}
		private void readStopWordFile(Path stopWordFile) {
			try {
				fis = new BufferedReader(new FileReader(stopWordFile.toString()));
				String stopWord = null;
				
				while ((stopWord = fis.readLine()) != null) {
					String deptFieldArray[] = stopWord.split("::");
					if(deptFieldArray[1].contains("Stanford"))
						DepartmentMap1.add(deptFieldArray[0].trim());
			} 
			}catch (IOException ioe) {
				System.err.println("Exception while reading stop word file '"
						+ stopWordFile + "' : " + ioe.toString());
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().length() > 0) {
				String arrEmpAttributes[] = value.toString().split("::");
				if(DepartmentMap1.contains(arrEmpAttributes[2].trim()))
				{
				try {
					
				strrating = arrEmpAttributes[3].toString().trim();
				} finally {
					if(strrating == "" )
						strrating="not found";
				}
				
				txtMapOutputKey.set(arrEmpAttributes[2].toString());
				txtMapOutputValue.set(strrating);

				context.write(txtMapOutputKey, txtMapOutputValue);
				strrating = "";
				
				}
			}
		}
	}
	
	// Driver program
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if (otherArgs.length != 3) {
	System.err.println("Usage: WordCount <input1-cache-business> <input2-review> <out>");
	System.exit(2);
	}
	// create a job with name "wordcount"
	Job job = new Job(conf, "StanfordBiz");
	job.setJarByClass(StanfordBiz.class);
	job.setMapperClass(Map.class);
	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(0);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	
	DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
			job.getConfiguration());
	
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	// set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	//Wait till job completion
	Path outputPath = new Path(args[2]);
	outputPath.getFileSystem(conf).delete(outputPath);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}