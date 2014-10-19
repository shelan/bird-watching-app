package org.ist;

/**
 * Created by shelan on 10/11/14.
 *
 *
 */

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    MapDriver mapDriver;
    ReduceDriver reduceDriver;
    MapDriver map2Driver;
    ReduceDriver reduce2Driver;

    public static final String INPUT_LOG_ENTRY = "Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.";

    @Before
    public void setUp() {
        Q3Mapper q3Mapper = new Q3Mapper();
        Q3Reducer q3Reducer = new Q3Reducer();
        mapDriver = MapDriver.newMapDriver(q3Mapper);
        reduceDriver = ReduceDriver.newReduceDriver(q3Reducer);
        
        Q2Mapper q2Mapper=new Q2Mapper();
        Q2Reducer q2Reducer=new Q2Reducer();
        map2Driver=MapDriver.newMapDriver(q2Mapper);
        reduce2Driver=ReduceDriver.newReduceDriver(q2Reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text(),new Text("Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0."));
        mapDriver.withOutput(new Text("eagle-25"),new Text("2013-10-09"));
        mapDriver.runTest();
   
        map2Driver.withInput(new LongWritable(),new Text("Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0."));
        map2Driver.withOutput(new Text("Q2"+"2013-10-09"+":"+"Sintra-1"),new Text("3"));
        map2Driver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> listofValues = new ArrayList<Text>();
        listofValues.add(new Text("2014-05-07"));
        listofValues.add(new Text("2014-05-08"));
        listofValues.add(new Text("2014-05-08"));
        listofValues.add(new Text("2014-05-09"));

        reduceDriver.withInput(new Text("eagle-25"),listofValues);
        reduceDriver.withOutput(new Text("eagle-25"),new Text("Thu Jan 09 00:05:00 WET 2014"));

        reduceDriver.runTest();
        
        List<Text>valuesForQ2=new ArrayList<Text>();
        valuesForQ2.add(new Text("5"));
        valuesForQ2.add(new Text("3"));
        
        reduce2Driver.withInput(new Text("Q22013-10-09:Sintra-1"),valuesForQ2);
        reduce2Driver.withOutput(new Text("2013-10-09:Sintra-1"),new Text("8.0"));
        reduce2Driver.runTest();
        
    }
}
