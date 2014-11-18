package org.ist.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.ist.QueryStatus;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

public class DBConnector {

    private static Log log = LogFactory.getLog(DBConnector.class);

    //Make sure to create the db in mysql before runing this. But u don't need to create tables
	static String dbUrl;
	static String userName;
	static String password;
    private Connection dbConnection = null;
    private Properties dbConnectionInfo = new Properties();


    public DBConnector() {
        try {
            readDbInfo();
            createConnection();
        } catch (IOException e) {
            log.fatal("Could not create DB connection. "+e);
        }

    }

	public Connection getDbConnection(){
        return dbConnection;
    }

    private void readDbInfo() throws IOException {
        InputStream input = null;
        try {
            input = getClass().getResourceAsStream("/db.properties");
            // load a properties file
            dbConnectionInfo.load(input);

        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private boolean createConnection(){
        try {
            // STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            dbUrl = "jdbc:mysql://" + dbConnectionInfo.getProperty("host") + ":3306/" + dbConnectionInfo.getProperty("dbname");
            userName = dbConnectionInfo.getProperty("dbuser");
            password = dbConnectionInfo.getProperty("userPassword");
            // STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            dbConnection = DriverManager.getConnection(dbUrl, userName, password);
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
		//String dbName = "TempQ1";// IF NOT EXISTS
		PreparedStatement preparedStatement = null;
		String createQuery = "CREATE TABLE IF NOT EXISTS " + Constants.Q1Table + "(DATE LONG NOT NULL, "
				+ "TOWER_ID VARCHAR(255) NOT NULL, " + "WING_SPAN INTEGER NOT NULL" + ")";

		try {
			preparedStatement = getDbConnection().prepareStatement(createQuery);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("table not created");
			return QueryStatus.ERROR;
		}
		String insertQuery = "insert into " + Constants.Q1Table+ " VALUES (?,?,?)";

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
		String dbName = "TempQ2";// IF NOT EXISTS
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
		String dbName = "TempQ3";// IF NOT EXISTS
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
