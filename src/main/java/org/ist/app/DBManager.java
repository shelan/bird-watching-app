
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

package org.ist.app;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;

public class DBManager {

    Connection dbConnection;

    public DBManager() {
        dbConnection = new DBConnector().getDbConnection();
    }
    public void storeFinalResults(){
        storeQ1FinalResult();
        storeQ2FinalResult();
        storeQ3FinalResult();
    }

    private void storeQ1FinalResult() {
        //String q1Table = "Q1_TABLE";// IF NOT EXISTS
        PreparedStatement preparedStatement = null;
        String dropQuery = "DROP TABLE IF EXISTS " + Constants.Q1_FINAL_TABLE;
        String createQuery = "CREATE TABLE " + Constants.Q1_FINAL_TABLE + " (DATE LONG NOT NULL, "
                + "TOWER_ID VARCHAR(255) NOT NULL " + ")";

        String getExistingResultQuery = "SELECT * FROM " + Constants.Q1_TABLE;

        try {
            preparedStatement = dbConnection.prepareStatement(dropQuery);
            preparedStatement.executeUpdate();
            preparedStatement = dbConnection.prepareStatement(createQuery);
            preparedStatement.executeUpdate();

            preparedStatement = dbConnection.prepareStatement(getExistingResultQuery);
            ResultSet results= preparedStatement.executeQuery();

            System.out.println("================ calculating final results Q1 ================");
            System.out.println("==================================================");

            Map<Long, Integer> existingResultMap = new Hashtable<Long, Integer>();
            Map<Long, String> filteredResults = new Hashtable<Long, String>();

            while (results.next()) {
                int wingSpan = results.getInt("WING_SPAN");
                String towerID = results.getString("TOWER_ID");
                long date = results.getLong("DATE");

                if(existingResultMap.get(date) == null) {
                    existingResultMap.put(date, wingSpan);
                    filteredResults.put(date, towerID);
                }
                else {
                    if(wingSpan > existingResultMap.get(date)) {
                        existingResultMap.put(date,wingSpan);
                        filteredResults.put(date, towerID);
                    }
                }
            }

           /* DateFormat q1Formatter = new SimpleDateFormat("yyyy-MM-dd");
            TimeZone.setDefault(TimeZone.getTimeZone("WEST"));*/

            System.out.printf("");

            for (Long date : filteredResults.keySet()) {
                //q1Formatter.parse(date).getTime();

                String insertQuery = "insert into " + Constants.Q1_FINAL_TABLE + " VALUES (?,?)";

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

    private void storeQ2FinalResult() {

        PreparedStatement preparedStatement = null;
        String dropQuery = "DROP TABLE IF EXISTS " + Constants.Q2_FINAL_TABLE;
        String createQuery = "CREATE TABLE " + Constants.Q2_FINAL_TABLE + "(DATE LONG NOT NULL, "
                + "TOWER_ID VARCHAR(255) NOT NULL, " + "TOTAL_WEIGHT INTEGER NOT NULL" + ")";

        String getExistingResultQuery = "SELECT * FROM " + Constants.Q2_TABLE;

        try {
            preparedStatement = dbConnection.prepareStatement(dropQuery);
            preparedStatement.executeUpdate();
            preparedStatement = dbConnection.prepareStatement(createQuery);
            preparedStatement.executeUpdate();

            preparedStatement = dbConnection.prepareStatement(getExistingResultQuery);
            ResultSet results= preparedStatement.executeQuery();

            System.out.println("================ calculating final results Q2 ================");
            System.out.println("==================================================");

            Map<String, Integer> existingResultMap = new Hashtable<String, Integer>();
            Map<String, Integer> filteredResults = new Hashtable<String, Integer>();

            while (results.next()) {
                long date = results.getLong("DATE");
                String towerID = results.getString("TOWER_ID");
                int weight = results.getInt("TOTAL_WEIGHT");
                String mapKey = String.valueOf(date).concat(Constants.SEPERATOR).concat(towerID);

                if(existingResultMap.get(mapKey) == null) {
                    existingResultMap.put(mapKey, weight);
                    filteredResults.put(mapKey, weight);
                } else {
                    int totalWeight = existingResultMap.get(mapKey) + weight;
                    existingResultMap.put(mapKey, totalWeight);
                    filteredResults.put(mapKey, totalWeight);
                }
            }

            for (String mapKey : filteredResults.keySet()) {
                String insertQuery = "insert into " + Constants.Q2_FINAL_TABLE + " VALUES (?,?,?)";
                String[] dateTower = mapKey.split(Constants.SEPERATOR);
                try {
                    preparedStatement = dbConnection.prepareStatement(insertQuery);
                    preparedStatement.setLong(1, Long.valueOf(dateTower[0]));
                    preparedStatement.setString(2, dateTower[1]);
                    preparedStatement.setInt(3, filteredResults.get(mapKey));
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

    private void storeQ3FinalResult() {

        PreparedStatement preparedStatement = null;
        String dropQuery = "DROP TABLE IF EXISTS " + Constants.Q3_FINAL_TABLE;
        String createQuery = "CREATE TABLE " + Constants.Q3_FINAL_TABLE + "(LAST_SEEN LONG NOT NULL, "
                + "BIRD_ID VARCHAR(250) NOT NULL" + ")";

        String getExistingResultQuery = "SELECT * FROM " + Constants.Q3_TABLE;

        try {
            preparedStatement = dbConnection.prepareStatement(dropQuery);
            preparedStatement.executeUpdate();
            preparedStatement = dbConnection.prepareStatement(createQuery);
            preparedStatement.executeUpdate();

            preparedStatement = dbConnection.prepareStatement(getExistingResultQuery);
            ResultSet results= preparedStatement.executeQuery();

            System.out.println("================ calculating final results Q3 ================");
            System.out.println("==================================================");

            Map<String, Long> existingResultMap = new Hashtable<String, Long>();
            Map<String, Long> filteredResults = new Hashtable<String, Long>();

            while (results.next()) {
                long lastSeen = results.getLong("LAST_SEEN");
                String birdId = results.getString("BIRD_ID");

                if(existingResultMap.get(birdId) == null) {
                    existingResultMap.put(birdId, lastSeen);
                    filteredResults.put(birdId, lastSeen);
                } else {
                    if(lastSeen > existingResultMap.get(birdId))
                    existingResultMap.put(birdId, lastSeen);
                    filteredResults.put(birdId, lastSeen);
                }
            }

            for (String birdId : filteredResults.keySet()) {
                String insertQuery = "insert into " + Constants.Q3_FINAL_TABLE + " VALUES (?,?)";
                try {
                    preparedStatement = dbConnection.prepareStatement(insertQuery);
                    preparedStatement.setLong(1, filteredResults.get(birdId));
                    preparedStatement.setString(2, birdId);
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
