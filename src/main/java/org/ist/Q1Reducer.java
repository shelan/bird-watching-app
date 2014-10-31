package org.ist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ashansa on 10/10/14.
 */
public class Q1Reducer extends Reducer<Text, Text, Text, Text>{

    private static Log log = LogFactory.getLog(Q1Reducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //Q1
        //initializing a text with 0 span
        Text biggerSpan = new Text("testElement:0");
        while (values.iterator().hasNext()){
            Text tempBigger = getBiggestSpan(biggerSpan, values.iterator().next());
            biggerSpan.set(tempBigger);
        }


        context.write(key,biggerSpan);
    }

    private Text getBiggestSpan(Text value1, Text value2) {
        String s1 = value1.toString(); // <towerID-span> ie : Sintra1-20
        String s2 = value2.toString();

        return Integer.valueOf(s1.substring(s1.lastIndexOf(":")+1)) >
                Integer.valueOf(s2.substring(s2.lastIndexOf(":")+1))?
                value1 : value2;
    }
}
