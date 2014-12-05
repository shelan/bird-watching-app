
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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer extends Reducer<Text, Text, Text, Text>
{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{	//SUFFIX of 2 strings to indicate question
		DBConnector connector=new DBConnector();
		String[] keyStrings=key.toString().substring(2,key.toString().length()).split(Utils.KEY_SEPERATOR);
		Text sumWeight = new Text();
		float sum = 0;
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		TimeZone.setDefault(TimeZone.getTimeZone("WET"));
		try{	
		Date date = formatter.parse(keyStrings[0]);
		String towerId=keyStrings[1];
		key=new Text(keyStrings[0]+":"+keyStrings[1]);
		for (Text value : values)
		{
		sum += Float.parseFloat(value.toString());
		}
		//here we have to save the date|tower_id|sum weight to the database
		connector.executeQueryForQ2(date, towerId, sum);
		}
		catch (Exception ex)
		{
		System.out.println("Can't parse weight as a float value");
		}
		sumWeight.set(String.valueOf(sum));
		context.write(key, sumWeight);
	}
}
