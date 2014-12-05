
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Log log = LogFactory.getLog(Q1Mapper.class);

    public void map(LongWritable key, Text value, Context output) throws IOException,
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
                String outputValue = logDetails[0] + ":" + logDetails[5]; // add towerID and span
                output.write(new Text(logDetails[1]), new Text(outputValue));
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid input log, cannot process " + value.toString() + " " + e.getMessage());
            return;
        }
    }
}
