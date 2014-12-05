
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
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q2Mapper for Given a date and a tower-id print the total added estimated weight of all the birds seen by a tower.
 *  ip: Sintra-1(tower-id),2013-10-09(date),17:54.01(time),eagle-25(bird-id),3(weight),120(span),0(wc)
 */
public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text>
{
	private static Log log = LogFactory.getLog(Q2Mapper.class);
	private Text keyDateTid = new Text();
	private Text birdWeight = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split(",");
		if (tokens.length != 7)
		{
			log.error("input file is not valid");
		}
		for (int i = 0; i < tokens.length; i++)
		{
			tokens[i] = tokens[i].trim();
		}

		try
		{
			// If any of them is null, record is a garbage so that we don't need to process further
			if (!tokens[0].isEmpty() && !tokens[1].isEmpty() && !tokens[4].isEmpty() && Float.parseFloat(tokens[4]) != 0)
			{
				//Q2date:towerid
				String dateTidCombined = tokens[1].concat(Utils.KEY_SEPERATOR).concat(tokens[0]);
				keyDateTid.set("Q2"+dateTidCombined);
				birdWeight.set(tokens[4]);
				context.write(keyDateTid, birdWeight);
			}
		} catch (Exception ex)
		{
			log.error("input file is not in correct format");
		}

	}
}
