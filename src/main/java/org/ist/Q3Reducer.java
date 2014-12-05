
/*
 *
 *  * Copyright 2014
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.ist;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class Q3Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text ,Text> {

    /**
     * Q3Reducer Function
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
        System.out.println("token of :"+key);
        for (Text value : values) {
            //System.out.println("token:"+ key + " value :" + value );
            try {
                System.out.println("last seen date"+lastSeenDate);
               Date newDate = formatter.parse(String.valueOf(value));
                System.out.println("formatted date"+newDate.toString()+"\n");
               long newTime = newDate.getTime();

               if(lastSeenDate < newTime){
                     lastSeenDate = newTime;
                }
               // System.out.println("last seen date "+ (new Date(lastSeenDate).toString()) );
            } catch (ParseException e) {
                System.out.println("Error while parsing the date field");
            }
        }

        lastSeenDateString.set(formatter.format(new Date(lastSeenDate)));
        context.write(key,lastSeenDateString);
    }
}
