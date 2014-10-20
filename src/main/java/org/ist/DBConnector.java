package org.ist;

import org.ist.QueryStatus;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBConnector {
    //Make sure to create the db in mysql before runing this. But u don't need to create tables
	static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/BirdWatchingDB"; 
	static final String USER = "root";
	static final String PASS = "";

	private static Connection getConnection() {
		Connection conn = null;
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected database successfully...");
		} catch (Exception ex) {
			System.out.println("error while establishing the connection");
			ex.printStackTrace();
		}
		return conn;
	}

	// date expect an java.sql.Date object
	public QueryStatus executeQueryForQ1(Date date, String towerId, double wingSpan) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q1Table";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(DATE DATE NOT NULL, "
				+ "TOWER_ID VARCHAR(255) NOT NULL, " + "WING_SPAN BIGINT NOT NULL" + ")";

		try {
			preparedStatement = getConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?,?)";

		try {
			preparedStatement = getConnection().prepareStatement(insertQuery);
			preparedStatement.setDate(1, date);
			preparedStatement.setString(2, towerId);
			preparedStatement.setDouble(3, wingSpan);
			preparedStatement.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;

		}

		return QueryStatus.SUCCESS;
	}

	public QueryStatus executeQueryForQ2(Date date, String towerId, float totalWeight) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q2Table";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(DATE DATE NOT NULL, "
				+ "TOWER_ID VARCHAR(255) NOT NULL, " + "TOTAL_WEIGHT FLOAT NOT NULL" + ")";

		try {
			preparedStatement = getConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?,?)";

		try {
			preparedStatement = getConnection().prepareStatement(insertQuery);
			preparedStatement.setDate(1, date);
			preparedStatement.setString(2, towerId);
			preparedStatement.setFloat(3, totalWeight);
			preparedStatement.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;

		}

		return QueryStatus.SUCCESS;
	}

	// date expect an java.sql.Date object
	public QueryStatus executeQueryForQ3(String birdId, Date lastSeen) {
		// Check whether database exist/ If not create and insert. If exist only
		// insert
		String dbName = "Q3Table";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + dbName + "(LAST_SEEN DATE NOT NULL, "
				+ "BIRD_ID VARCHAR(250) NOT NULL" + ")";

		try {
			preparedStatement = getConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + dbName + " VALUES (?,?)";

		try {
			preparedStatement = getConnection().prepareStatement(insertQuery);
			preparedStatement.setDate(1, lastSeen);
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
