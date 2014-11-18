package org.ist.app;

import org.ist.*;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by ashansa on 11/17/14.
 */
public class DBManager {

    Connection dbConnection;

    public DBManager() {
        dbConnection = new DBConnector().getDbConnection();
    }
    public void storeFinalResults(){
        storeQ1FinalResult();
    }

    private void storeQ1FinalResult() {
        //String q1Table = "Q1Table";// IF NOT EXISTS
        PreparedStatement preparedStatement = null;
        String dropQuery = "DROP TABLE IF EXISTS " + Constants.Q1FinalTable;
        String createQuery = "CREATE TABLE " + Constants.Q1FinalTable + " (DATE LONG NOT NULL, "
                + "TOWER_ID VARCHAR(255) NOT NULL " + ")";

        String getExistingResultQuery = "SELECT * FROM " + Constants.Q1Table;

        try {
            preparedStatement = dbConnection.prepareStatement(dropQuery);
            preparedStatement.executeUpdate();
            preparedStatement = dbConnection.prepareStatement(createQuery);
            preparedStatement.executeUpdate();

            preparedStatement = dbConnection.prepareStatement(getExistingResultQuery);
            ResultSet results= preparedStatement.executeQuery();

            System.out.println("================ calculating final results ================");
            System.out.println("==================================================");

            Map<Long, Integer> dateSpanMap = new Hashtable<Long, Integer>();
            Map<Long, String> filteredResults = new Hashtable<Long, String>();

            while (results.next()) {
                int wingSpan = results.getInt("WING_SPAN");
                String towerID = results.getString("TOWER_ID");
                long date = results.getLong("DATE");

                if(dateSpanMap.get(date) == null) {
                    dateSpanMap.put(date,wingSpan);
                    filteredResults.put(date, towerID);
                }
                else {
                    if(wingSpan > dateSpanMap.get(date)) {
                        dateSpanMap.put(date,wingSpan);
                        filteredResults.put(date, towerID);
                    }
                }
            }

           /* DateFormat q1Formatter = new SimpleDateFormat("yyyy-MM-dd");
            TimeZone.setDefault(TimeZone.getTimeZone("WEST"));*/

            System.out.printf("");

            for (Long date : filteredResults.keySet()) {
                //q1Formatter.parse(date).getTime();

                String insertQuery = "insert into " + Constants.Q1FinalTable + " VALUES (?,?)";

                try {
                    preparedStatement = dbConnection.prepareStatement(insertQuery);
                    preparedStatement.setLong(1, date);
                    preparedStatement.setString(2, filteredResults.get(date));
                    preparedStatement.executeUpdate();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.out.println("table not created");
                }
            }

            System.out.println("==================================================");
            System.out.println("==================================================");
            // results.
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("table not created");
        }
    }
}
