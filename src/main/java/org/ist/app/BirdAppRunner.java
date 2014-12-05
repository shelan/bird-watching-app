
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

package org.ist.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ist.Q2Mapper;
import org.ist.Q2Reducer;

public class BirdAppRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception
    {
        try{
            int res = ToolRunner.run(new Configuration(), new BirdAppRunner(),
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
        job.setJarByClass(BirdAppRunner.class);
        job.setMapperClass(BirdAppMapper.class);
        job.setCombinerClass(BirdAppCombiner.class);
        job.setReducerClass(BirdAppReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        new DBManager().storeFinalResults();
        return 0;
    }

}
