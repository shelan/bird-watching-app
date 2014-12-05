
/*
 *
 *  * Copyright 2014
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
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
