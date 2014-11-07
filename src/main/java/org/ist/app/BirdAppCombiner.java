package org.ist.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ist.Utils;

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
public class BirdAppCombiner extends Reducer<Text, Text, Text, Text> {

    private static Log log = LogFactory.getLog(BirdAppCombiner.class);

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {
            int keyPrefix = Integer.valueOf(key.toString().substring(0,1));
            switch (keyPrefix) {
                case 1:
                    Text biggerSpan = new Text("testElement:0");
                    while (values.iterator().hasNext()){
                        Text tempBigger = getBiggestSpan(biggerSpan, values.iterator().next());
                        biggerSpan.set(tempBigger);
                    }
                    context.write(key,biggerSpan);
                    break;

                case 2:
                    Text sumWeight = new Text();
                    int sum = 0;
                    for (Text value : values) {
                        sum += Integer.valueOf(value.toString());
                    }
                    //here we have to save the date|tower_id|sum weight   to the database
                    sumWeight.set(String.valueOf(sum));
                    context.write(key, sumWeight);
                    break;

                case 3:
                    DateFormat q3Formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    TimeZone.setDefault(TimeZone.getTimeZone("WEST"));
                    Text lastSeenDateString = new Text();
                    //initialized to epoc time
                    long lastSeenDate = 0L;
                    //System.out.println("token of :"+key);
                    for (Text value : values) {
                        //System.out.println("last seen date"+lastSeenDate);
                        Date newDate = q3Formatter.parse(String.valueOf(value));
                        //System.out.println("formatted date"+newDate.toString()+"\n");
                        long newTime = newDate.getTime();

                        if(lastSeenDate < newTime){
                            lastSeenDate = newTime;
                        }
                    }
                    lastSeenDateString.set(q3Formatter.format(new Date(lastSeenDate)));
                    context.write(key,lastSeenDateString);
                    break;
            }
        } catch (NumberFormatException e) {
            log.error(e.getMessage(), e);
            return;
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
        }
    }

    private Text getBiggestSpan(Text value1, Text value2) {
        String s1 = value1.toString(); // <towerID-span> ie : Sintra1-20
        String s2 = value2.toString();

        return Integer.valueOf(s1.substring(s1.lastIndexOf(":")+1)) >
                Integer.valueOf(s2.substring(s2.lastIndexOf(":")+1))?
                value1 : value2;
    }
}
