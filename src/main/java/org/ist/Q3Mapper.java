package org.ist;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by shelan on 10/11/14.
 * <p/>
 * input file format
 * Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.
 * <p/>
 * Q3Mapper for "List all tagged birds that have not been observed for more than one week"
 */
public class Q3Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    private Text birdToken = new Text();
    private Text lastSeenDate = new Text();

    /**
     * Q3Mapper function
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        int index = 0;
        String date = "";
        String time = "";
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();

            //setting date which is 2nd entry
            if (index == 1) {
               date = token.trim();
            }
            //setting time which is 3rd entry
            else if ( index == 2) {
                time = token.trim();
            }
            //getting 4th log entry as token
            else if (index == 3) {
                birdToken.set("Q3"+token.trim());
                lastSeenDate.set(date+" "+time);
                context.write(birdToken, lastSeenDate);
            }
            index++;
        }
    }

}
