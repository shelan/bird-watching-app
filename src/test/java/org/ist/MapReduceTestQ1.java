package org.ist;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;


public class MapReduceTestQ1 {

    MapDriver mapDriver;
    ReduceDriver reduceDriver;
    ReduceDriver combinerDriver;

    public static final String INPUT_LOG_ENTRY = "Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 2";

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        Q1Reducer reducer = new Q1Reducer();
        Q1Combiner combiner = new Q1Combiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(),new Text(INPUT_LOG_ENTRY));

        String expectedResult = "Sintra-1:120";
        mapDriver.withOutput(new Text("2013-10-09"), new Text(expectedResult));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        ArrayList<Text> listofValues = new ArrayList<Text>();
        listofValues.add(new Text("tower1:07"));
        listofValues.add(new Text("tower2:18"));
        listofValues.add(new Text("tower3:15"));
        listofValues.add(new Text("tower5:329"));
        listofValues.add(new Text("tower3:15"));
        listofValues.add(new Text("tower2:183"));
        listofValues.add(new Text("tower3:1"));

        reduceDriver.withInput(new Text("2013-10-09"),listofValues);
        reduceDriver.withOutput(new Text("2013-10-09"),new Text("tower5:329"));

        reduceDriver.runTest();
    }

    @Test
    public void testCombiner() throws IOException {
        ArrayList<Text> listofValues = new ArrayList<Text>();
        listofValues.add(new Text("tower1:507"));
        listofValues.add(new Text("tower2:18"));
        listofValues.add(new Text("tower3:15"));
        listofValues.add(new Text("tower5:329"));
        listofValues.add(new Text("tower3:15"));
        listofValues.add(new Text("tower2:183"));
        listofValues.add(new Text("tower3:1"));

        combinerDriver.withInput(new Text("2013-10-09"),listofValues);
        combinerDriver.withOutput(new Text("2013-10-09"),new Text("tower1:507"));

        combinerDriver.runTest();
    }
}
