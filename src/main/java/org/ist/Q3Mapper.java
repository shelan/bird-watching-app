package org.ist;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by shelan on 10/11/14.
 *
 * input file format
 * Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.
 *
 * Q3Mapper for "List all tagged birds that have not been observed for more than one week"
 */
public class Q3Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text> {

    private Text birdToken = new Text();
    private Text lastSeenDate = new Text();

    /**
     *  Q3Mapper function
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(),",");
        int index = 0;
        String date;
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();

            //setting date which is 2nd entry
            if(index == 1){
                lastSeenDate.set(token.trim());
            }

            //getting 3rd log entry as token
            if(index == 3) {
                birdToken.set(token.trim());
                context.write(birdToken, lastSeenDate);
            }
            index ++;
        }
    }

}
