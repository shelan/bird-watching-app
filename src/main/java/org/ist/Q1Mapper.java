package org.ist;
/**
 * Created by ashansa on 10/10/14.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q1Mapper extends Mapper<Text, Text, Text, Text> {

    private static Log log = LogFactory.getLog(Q1Mapper.class);

    public void map(Text key, Text value, Context output) throws IOException,
            InterruptedException {
        //value : towerId,date,time,birdId,weight,span,weather  & whether code:0: sunny; 1: cloudy; 2: raining; 3: snowing
        //ie : Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0
        String[] logDetails = value.toString().split(",");

        // trim the elements
        for (int i = 0; i < logDetails.length ; i++) {
            logDetails[i] = logDetails[i].trim();
        }

        if(logDetails.length != 7) {
            log.warn("Invalid input log, cannot process " + value.toString());
            return;
        }

        try{
            //Q1: Given a date, display the tower that observed the bird with biggest wingspan in raining conditions.

            //filter out rainy days
            if(Integer.valueOf(logDetails[6]) == 2){
                String outputValue = logDetails[0] + "-" + logDetails[5]; // add towerID and span
                output.write(new Text(logDetails[1]), new Text(outputValue));
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid input log, cannot process " + value.toString() + " " + e.getMessage());
            return;
        }
    }
}
