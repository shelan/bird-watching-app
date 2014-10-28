package org.ist.app;

import org.ist.QueryStatus;
import java.sql.*;

public class DBConnector {
    //Make sure to create the db in mysql before runing this. But u don't need to create tables
	static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/BirdWatchingDB"; 
	static final String USER = "root";
	static final String PASS = "";
    private Connection dbConnection = null;


    public DBConnector(){
        createConnection();
    }

	public Connection getDbConnection(){
        return dbConnection;
    }

    private boolean createConnection(){
        try {
            // STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            // STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            dbConnection = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");
        } catch (Exception ex) {
            System.out.println("error while establishing the connection");
            ex.printStackTrace();
        }
        return (dbConnection!=null);
    }

	// date expect an java.sql.Date object
	public QueryStatus executeQueryForQ1(long date, String towerId, int wingSpan) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q1Table";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(DATE LONG NOT NULL, "
				+ "TOWER_ID VARCHAR(255) NOT NULL, " + "WING_SPAN INTEGER NOT NULL" + ")";

		try {
			preparedStatement = getDbConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?,?)";

		try {
			preparedStatement = getDbConnection().prepareStatement(insertQuery);
			preparedStatement.setLong(1, date);
			preparedStatement.setString(2, towerId);
			preparedStatement.setInt(3, wingSpan);
			preparedStatement.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;

		}

		return QueryStatus.SUCCESS;
	}

	public QueryStatus executeQueryForQ2(long date, String towerId, int totalWeight) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q2Table";// IF NOT EXISTS
		PreparedStatement preparedStatementCreate = null;
		PreparedStatement preparedStatementInsert = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(DATE LONG NOT NULL, "
				+ "TOWER_ID VARCHAR(255) NOT NULL, " + "TOTAL_WEIGHT INTEGER NOT NULL" + ")";

		try {
			preparedStatementCreate = getDbConnection().prepareStatement(createQuery);
			preparedStatementCreate.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?,?)";

		try {
			preparedStatementInsert = getDbConnection().prepareStatement(insertQuery);
			preparedStatementInsert.setLong(1, date);
			preparedStatementInsert.setString(2, towerId);
			preparedStatementInsert.setInt(3, totalWeight);
			preparedStatementInsert.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;

		}

		return QueryStatus.SUCCESS;
	}

	// date expect an java.sql.Date object
	public QueryStatus executeQueryForQ3(String birdId, long lastSeen) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q3Table";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(LAST_SEEN LONG NOT NULL, "
				+ "BIRD_ID VARCHAR(250) NOT NULL" + ")";

		try {
			preparedStatement = getDbConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?)";

		try {
			preparedStatement = getDbConnection().prepareStatement(insertQuery);
			preparedStatement.setLong(1, lastSeen);
			preparedStatement.setString(2, birdId);
			preparedStatement.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;

		}
		return QueryStatus.SUCCESS;
	}
}
