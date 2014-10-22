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

/**
 * Created by shelan on 10/20/14.
 */
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
        return 0;
    }

}
