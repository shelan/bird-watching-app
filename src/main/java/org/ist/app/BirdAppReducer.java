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
public class BirdAppReducer extends Reducer<Text, Text, Text, Text> {

    private static Log log = LogFactory.getLog(BirdAppReducer.class);
    private static DBConnector connector = new DBConnector();

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
                    // key - date , value - towerID:span
                    Text biggerSpan = new Text("testElement:0");
                    while (values.iterator().hasNext()){
                        Text tempBigger = getBiggestSpan(biggerSpan, values.iterator().next());
                        biggerSpan.set(tempBigger);
                    }
                    DateFormat q1Formatter = new SimpleDateFormat("yyyy-MM-dd");
                    TimeZone.setDefault(TimeZone.getTimeZone("WET"));
                    // use this when saving to db
                    long dateQ1 = q1Formatter.parse(String.valueOf(key.toString())).getTime();

                    String value = biggerSpan.toString();
                    String towerID = value.substring(0,value.lastIndexOf(":"));
                    String span = value.substring(value.lastIndexOf(":")+1);
                    connector.executeQueryForQ1(dateQ1, towerID, Double.valueOf(span) );
                    context.write(key,biggerSpan);
                    break;

                case 2:
                    String[] keyStrings=key.toString().substring(1,key.toString().length()).split(Utils.KEY_SEPERATOR);
                    Text sumWeight = new Text();
                    float sum = 0;
                    DateFormat q2Formatter = new SimpleDateFormat("yyyy-MM-dd");
                    TimeZone.setDefault(TimeZone.getTimeZone("WET"));
                    // use this when saving to db
                    long dateQ2 = q2Formatter.parse(String.valueOf(keyStrings[0])).getTime();
                    String towerId = keyStrings[1];
                    key = new Text(keyStrings[0] + ":" + keyStrings[1]);
                    for (Text valueQ2 : values) {
                        sum += Float.parseFloat(valueQ2.toString());
                    }
                    //here we have to save the date|tower_id|sum weight to the database
                    connector.executeQueryForQ2(dateQ2, towerId, sum);
                    sumWeight.set(String.valueOf(sum));
                    context.write(key, sumWeight);
                    break;

                case 3:
                    DateFormat q3Formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    TimeZone.setDefault(TimeZone.getTimeZone("WET"));
                    Text lastSeenDateString = new Text();
                    //initialized to epoc time
                    long lastSeenDate = 0L;
                    System.out.println("token of :"+key);
                    for (Text valueQ3 : values) {
                            System.out.println("last seen date"+lastSeenDate);
                            long newTime = q3Formatter.parse(String.valueOf(valueQ3)).getTime();
                            System.out.println("formatted date"+newTime+"\n");

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
