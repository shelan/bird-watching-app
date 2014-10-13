package org.ist;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by shelan on 10/11/14.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text ,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        DateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
        Text lastSeenDateString = new Text();
        //initialized to epoc time
        Date lastSeenDate = new Date(0);
        for (Text value : values) {
            try {
               Date newDate = formatter.parse(String.valueOf(value));

               if(lastSeenDate.before(newDate)){
                     lastSeenDate = newDate;
                }
            } catch (ParseException e) {
                System.out.println("Error while parsing the date field");
            }
        }
        lastSeenDateString.set(lastSeenDate.toString());
        context.write(key,lastSeenDateString);
    }
}