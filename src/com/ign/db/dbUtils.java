package com.ign.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class dbUtils {

	public static void main(String [] args) {
		System.out.println(checkUrl("10.23.23.32:3306/DatabaseName"));
	}

	/**@param String url
	 * @param String user
	 * @param String password 
	 * @returns java.sql.Connection*/
	public static Connection connectToMySql(String url, String user, String password) throws Exception {
		Connection c = null;
		// Load the JDBC driver
		String driverName = "com.mysql.jdbc.Driver";
		Class.forName(driverName);
		
		// Make the connection to the database
		c = DriverManager.getConnection(url,user,password);
		return c;
	}
	
	/**@param String varchar - string to be formatted. 
	 * @param int capLength - number of characters string should be capped at.
	 * All double-quotes are replaced by single-quotes, and string is capped. */
	public static String formatVarchar(String varchar, int capLength){
//		varchar = String.format("%s", varchar.replaceAll("'", "''"));
		varchar = String.format("%s", varchar.replaceAll("\"", "'"));
		if(varchar.length() > capLength)
			varchar = varchar.substring(0, capLength);
		return varchar;
	}
	
	public static boolean checkArgs(String feedPath, String templateTable, String targetTable, String url){
		File feedFile = new File(feedPath);
		if(feedFile.exists() && feedFile.canRead() && feedFile.isFile()){
			if(templateTable.split("\\.").length == 2){
				if(targetTable.split("\\.").length == 2){
					if(url.contains(":") && url.contains("/")){
						return true;
					} else {System.err.println("url not formatted correctly");	}
				}else {System.err.println("target table not formatted correctly");}
			} else {System.err.println("template table not formatted correctly");}
		} else {System.err.println("feed file path unreachable");}
		
		return false;
	}
	
	public static boolean checkUrl(String url){
//		"jdbc:mysql://"
		if(url.contains(":") && url.contains("/") && url.split(":").length == 4 && url.contains(".")) {
			if(!url.contains("jdbc:mysql://")){
				System.err.println("urlMissing jdbc:mysql:// ... try appending jdbc:mysql:// onto the front of "+ url);
				return false;
			}
			return true;
		} else {
			System.err.println("database url not formatted correctly: " + url);
		}
		return false;
	}
	
	public static boolean CreateTable(String url, String user, String password, String templateTable, String targetTable) throws ClassNotFoundException, SQLException{
		String statamento = "create table "+targetTable+" as select * from "+templateTable+" limit 0";
	//	String statamento = "create table ingest_staging.itunesGames_out as select * from ingest_staging.itunesGames_template limit 0";
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