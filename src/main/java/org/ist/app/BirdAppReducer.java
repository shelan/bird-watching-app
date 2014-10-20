package org.ist.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by shelan on 10/11/14.
 */
public class BirdAppReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Q3Reducer Function
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone.setDefault(TimeZone.getTimeZone("WET"));
        Text lastSeenDateString = new Text();
        //initialized to epoc time
        long lastSeenDate = 0L;
        System.out.println("token of :" + key);
        for (Text value : values) {
            //System.out.println("token:"+ key + " value :" + value );
            try {
                System.out.println("last seen date" + lastSeenDate);
                Date newDate = formatter.parse(String.valueOf(value));
                System.out.println("formatted date" + newDate.toString() + "\n");
                long newTime = newDate.getTime();

                if (lastSeenDate < newTime) {
                    lastSeenDate = newTime;
                }
                // System.out.println("last seen date "+ (new Date(lastSeenDate).toString()) );
            } catch (ParseException e) {
                System.out.println("Error while parsing the date field");
            }
        }

        lastSeenDateString.set(formatter.format(new Date(lastSeenDate)));
        context.write(key, lastSeenDateString);
    }
}
