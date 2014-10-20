package org.ist.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.ist.Utils;

import java.io.IOException;

/**
 * Created by shelan on 10/11/14.
 * <p/>
 * input file format
 * Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.
 * <p/>
 * Q3Mapper for "List all tagged birds that have not been observed for more than one week"
 */

public class BirdAppMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    private static Log log = LogFactory.getLog(BirdAppMapper.class);

    private Text dateKey = new Text();
    private Text wingSpanTowerIdCompositeVal = new Text();

    private Text dateTowerIdCompositeKey = new Text();
    private Text birdWeight = new Text();

    private Text birdIdKey = new Text();
    private Text timeStamp = new Text();

    /**
     * Q3Mapper function
     *
     * @param key line number
     * @param value line
     * @param output context output
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String[] logSplitValues = value.toString().split(",");
        // trim the elements
        for (int i = 0; i < logSplitValues.length; i++) {
            logSplitValues[i] = logSplitValues[i].trim();
        }
        if (!validateEntry(logSplitValues)) {
            return;
        }

        try {
            //Q1: Given a date, display the tower that observed the bird with biggest wingspan in raining conditions.
            //filter out rainy days
            if (Integer.valueOf(logSplitValues[6]) == 2) {
                String outputValue = logSplitValues[0] + ":" + logSplitValues[5]; // add towerID and span
                dateKey.set("1" + logSplitValues[1]);
                wingSpanTowerIdCompositeVal.set(outputValue);
                output.write(dateKey, wingSpanTowerIdCompositeVal);
            }

            //Q2: for Given a date and a tower-id print the total added estimated weight of all the birds seen by a tower.

            String dateTidCombined = (logSplitValues[1]).concat(Utils.KEY_SEPERATOR).concat(logSplitValues[0]);
            dateTowerIdCompositeKey.set("2"+ dateTidCombined);
            birdWeight.set(logSplitValues[4]);
            output.write(dateTowerIdCompositeKey, birdWeight);

            //Q3:
            birdIdKey.set(logSplitValues[3]);
            timeStamp.set(logSplitValues[1] + " " + logSplitValues[2]);
            output.write(birdIdKey,timeStamp);


        } catch (NumberFormatException e) {
            log.warn("Invalid input log, cannot process " + value.toString() + " " + e.getMessage());
        }


    }

    private boolean validateEntry(String[] values) {
        if (values.length == 7) {
            for (String value : values) {
                if(value.isEmpty()){
                    return false;
                }
            }
            // if (!tokens[0].isEmpty() && !tokens[1].isEmpty() && !tokens[4].isEmpty() && Float.parseFloat(tokens[4]) != 0)
        }
        return true;
    }

}
