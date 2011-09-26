package com.ign;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;



public class Sandbox {
	String fromTable = "itunesGames_template";
	String toTable = "itunesGames_out";
	String url1 = "jdbc:mysql://10.92.217.17:3306/ingest_staging:user=mdadmin;password=l3m0n";
	String url2 = "jdbc:mysql://mdadmin@10.92.217.17:3306/ingest_staging:password=l3m0n";
	String url3 = "jdbc:mysql://mdadmin;l3m0n@10.92.217.17:3306/ingest_staging";
	String url4 = "jdbc:mysql://user=mdadmin;password=l3m0n@10.92.217.17:3306/ingest_staging";
	String url5 = "jdbc:mysql://10.92.217.17:3306/ingest_staging:user=mdadmin;password=l3m0n;";
	String url6 = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Sandbox sandy = new Sandbox();
//		OpenMySQL(sandy.url6, "mdadmin", "l3m0n");
		System.out.println(CreateTable(sandy.url6, "mdadmin","l3m0n", sandy.fromTable, sandy.toTable));
	}
	
	private static Connection OpenMySQL(String url, String user, String password) throws Exception {
        Connection c = null;
        // Load the JDBC driver
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);

        // Make the connection to the database
        c = DriverManager.getConnection(url, user, password);
        return c;
    }
	
	private static boolean CreateTable(String url, String user, String password, String fromTable, String newTable) throws ClassNotFoundException, SQLException{
		String statamento = "create table ingest_staging."+newTable+" as select * from ingest_staging."+fromTable+" limit 0";
//		String statamento = "create table ingest_staging.itunesGames_out as select * from ingest_staging.itunesGames_template limit 0";
		Connection c = null;

        // Load the JDBC driver
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);
        c = DriverManager.getConnection(url, user, password);
        java.sql.Statement statement = c.createStatement();
        try{
        	int rs = statement.executeUpdate(statamento);
        	System.out.println(rs);
        } catch(SQLException e){
        	System.out.println(e.getMessage());
        	return false;
        }
        statement.close();
        c.close();
		return true;
	}
}
