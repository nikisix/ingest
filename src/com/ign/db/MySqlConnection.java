package com.ign.db;

import java.sql.SQLException;

public class MySqlConnection {
	String url;
	String username;
	String password;
	String tablename;
	
	String database;
	String address;
	String port;
	
	/**@author ntomasino
	 * @param String url -- example: jdbc:mysql://10.92.217.17:3306/ingest_staging
	 * @param String username
	 * @param String password
	 * @param String tablename
	 * */
	public MySqlConnection(String url1, String username1, String password1, String tablename1) throws SQLException{
		if(dbUtils.checkUrl(url1)){
			url = url1;
			username = username1;
			password = password1;
			tablename = tablename1;
			setAddress();
		} else {
			throw new SQLException(url1 + " URL not formatted correctly");
		}
	}
	/**
	 * @param String url -- example: jdbc:mysql://10.92.217.17:3306/ingest_staging
	 * @param String username
	 * @param String password
	 * @param String tablename
	 * @param String database
	 * @param String address
	 * @param String port
	 * */
	public MySqlConnection(String url1, String username1, String password1, String tablename1, String database1, String address1, String port1) throws SQLException{
		if(dbUtils.checkUrl(url1)){
			url = url1;
			username = username1;
			password = password1;
			tablename = tablename1;
			database = database1;
			address = address1;
			port = port1;
		} else {
			throw new SQLException(url1 + " URL not formatted correctly");
		}
	}
	
	private boolean setAddress(){
//		jdbc example url   jdbc:mysql://10.92.217.17:3306/ingest_staging
		String tail = url.split("//")[1];
		address = tail.split(":")[0];
		String portAndDatabase = tail.split(":")[1];
		port = portAndDatabase.split("/")[0];
		database = portAndDatabase.split("/")[1];
		return true;
	}

	@Override
	public String toString(){
		String n = "\n";
		String ret = 
			"url: "+url +n+ 
			"username: "+username +n+
			"password: "+password +n+
			"tablename: "+tablename +n+
			"database: "+database +n+
			"address: "+address +n+
			"port: "+port +n;
		return ret;
	}
	
	public static void main(String ... args) throws SQLException{
		String myUrl = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
		MySqlConnection mysqlcon = new MySqlConnection(myUrl, "username", "password", "tablename");
		System.out.println(mysqlcon.toString());
	}
	
	public String getUrl(){
		return url;
	}
	public String getUsername(){
		return username;
	}
	public String getPassword(){
		return password;
	}
	public String getTablename(){
		return tablename;
	}
	public String getAddress(){
		return address;
	}
	public String getDatabase(){
		return database;
	}
	public String getPort(){
		return port;
	}
}
