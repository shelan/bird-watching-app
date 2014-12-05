package org.ist;

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

    MapDriver map3Driver;
    ReduceDriver reduce3Driver;
    MapDriver map2Driver;
    ReduceDriver reduce2Driver;

    public static final String INPUT_LOG_ENTRY = "Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.";

    @Before
    public void setUp() {
        Q3Mapper q3Mapper = new Q3Mapper();
        Q3Reducer q3Reducer = new Q3Reducer();
        map3Driver = MapDriver.newMapDriver(q3Mapper);
        reduce3Driver = ReduceDriver.newReduceDriver(q3Reducer);
        
        Q2Mapper q2Mapper=new Q2Mapper();
        Q2Reducer q2Reducer=new Q2Reducer();
        map2Driver=MapDriver.newMapDriver(q2Mapper);
        reduce2Driver=ReduceDriver.newReduceDriver(q2Reducer);
    }

    @Test
    public void testMapper() throws IOException {
        map3Driver.withInput(new LongWritable(), new Text("Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0."));
        map3Driver.withOutput(new Text("Q3eagle-25"), new Text("2013-10-09 17:54.01"));
        map3Driver.runTest();
   
        map2Driver.withInput(new LongWritable(),new Text("Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0."));
        map2Driver.withOutput(new Text("Q2"+"2013-10-09"+":"+"Sintra-1"),new Text("3"));
        map2Driver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> listofValues = new ArrayList<Text>();
        listofValues.add(new Text("2014-02-07 18:54:01"));
        listofValues.add(new Text("2014-03-08 16:54:01"));
        listofValues.add(new Text("2014-05-09 15:54:01"));
        listofValues.add(new Text("2014-05-09 14:54:01"));

        reduce3Driver.withInput(new Text("Q3eagle-25"),listofValues);
        reduce3Driver.withOutput(new Text("Q3eagle-25"),new Text("2014-05-09 15:54:01"));

        reduce3Driver.runTest();
        
        List<Text>valuesForQ2=new ArrayList<Text>();
        valuesForQ2.add(new Text("5"));
        valuesForQ2.add(new Text("3"));
        
        reduce2Driver.withInput(new Text("Q22013-10-09:Sintra-1"), valuesForQ2);
        reduce2Driver.withOutput(new Text("2013-10-09:Sintra-1"), new Text("8.0"));

        reduce2Driver.runTest();
        
    }
}
