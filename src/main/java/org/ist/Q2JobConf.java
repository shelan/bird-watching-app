/**
 * 
 */
package org.ist;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.tools.javac.resources.javac;

/**
 * @author chathuri
 * 
 */
public class Q2JobConf extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		try{
		int res = ToolRunner.run(new Configuration(), new Q2JobConf(),
				args);
				System.exit(res);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		
		
	}

	
	private static void addJarToDistributedCache(
	        Class classToAdd, Configuration conf)
	    throws IOException {
	 try{
	    // Retrieve jar file for class2Add
	    String jar = classToAdd.getProtectionDomain().
	            getCodeSource().getLocation().
	            getPath();
	    File jarFile = new File(jar);
	 
	    // Declare new HDFS location
	    Path hdfsJar = new Path("lib/"
	            + jarFile.getName());
	 
	    // Mount HDFS
	    FileSystem hdfs = FileSystem.get(conf);
	 
	    // Copy (override) jar file to HDFS
	    hdfs.copyFromLocalFile(false, true,
	        new Path(jar), hdfsJar);
	 
	    // Add jar to distributed classPath
	    DistributedCache.addFileToClassPath(hdfsJar, conf);
	 }
	 catch(Exception ex){
		 ex.printStackTrace();
	 }
	}


	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//addJarToDistributedCache(java.sql.Date.class, conf);
	     Job job = Job.getInstance(getConf(), "bird watch");
		job.setJarByClass(Q2JobConf.class);
		job.setMapperClass(Q2Mapper.class);
		//job.setCombinerClass(Q2Reducer.class);
		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		   return 0;
	}
}
