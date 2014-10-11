package org.ist;

/**
 * Created by shelan on 10/11/14.
 *
 *
 */

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    MapDriver mapDriver;
    ReduceDriver reduceDriver;

    public static final String INPUT_LOG_ENTRY = "Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0.";

    @Before
    public void setUp() {
        Mapper mapper = new Mapper();
        Reducer reducer = new Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text(),new Text("Sintra-1, 2013-10-09, 17:54.01, eagle-25, 3, 120, 0."));
        mapDriver.withOutput(new Text("eagle-25"),new Text("2013-10-09"));
        mapDriver.runTest();
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
    }
}
