package com.ign.scrape.description;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;

import com.ign.db.MySqlConnection;
import com.ign.db.dbUtils;

public class DescriptionFileToDatabase {

	/**@author ntomasino
	 * @param String gameIdFile
	 * @param String gameDescriptionFile
	 */
	
	Hashtable idDescHash = new Hashtable();
	String gameIdFilePath, gameDescFilePath;
	String url, username, password, tablename;
	MySqlConnection sqlCxn;
	Connection cxn;
	public static void main(String[] args) throws Exception {
		final String help = "DescriptionFileToDatabase <gameId file> <game description file> \n" +
				"DescriptionFileToDatabase <gameIdFile> <gameDescFile> <databaseUrl> <dbUsername> <dbPassword> <tablename> \n" +
				"<gameId file> - contains one gameId (java long) per line.\n" +
				"<game description file> - contains one description per line (capped at 300 characters).\n" +
				"<databaseUrl> - default: jdbc:mysql://10.92.217.17:3306/ingest_staging \n" +
				"<tablename> - default: descriptionBuffer \n";
		
		DescriptionFileToDatabase dfdb = new DescriptionFileToDatabase();
		
		if (com.ign.utils.checkArgs(args, 2, help)){
			System.out.println("Using defaults for databaseUrl, username, password, and tablename");
			dfdb.gameIdFilePath = args[0]; 
			dfdb.gameDescFilePath = args[1];
			dfdb.url = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
			dfdb.username = "mdadmin";
			dfdb.password = "l3m0n";
			dfdb.tablename = "descriptionBuffer";
		} else if (com.ign.utils.checkArgs(args, 6, help)) {
			dfdb.gameIdFilePath = args[0]; 
			dfdb.gameDescFilePath = args[1];
			dfdb.url = args[2];
			dfdb.username = args[3];
			dfdb.password = args[4];
			dfdb.tablename = args[5];
		} else {
			System.err.println("Wrong number of arguments\n" + help);
			return;
		}
				
		dfdb.sqlCxn = new MySqlConnection(dfdb.url, dfdb.username, dfdb.password, dfdb.tablename);

//		dfdb.loadGameIdsAndDescriptions();
		dfdb.writeToDatabase();
	}
		
	/**read in files
	 * create database connection
	 * walk through the files and insert into the database per line
	 * insert into tablename (GameId,Description) values (gameId,"description") 
	 * @throws Exception */
	
		public void writeToDatabase() throws Exception{
		BufferedReader idReader = null, descReader = null;
		try {
			idReader = new BufferedReader(new FileReader(gameIdFilePath));
		} catch (FileNotFoundException e) {
			System.err.println("DescriptionFileToDatabase.java: Couldn't read in the <game Id file>\n");
			e.printStackTrace();
		}
		
		try {
			descReader = new BufferedReader(new FileReader(gameDescFilePath));
		} catch (FileNotFoundException e) {
			System.err.println("DescriptionFileToDatabase.java: Couldn't read in the <game description file>\n");
			e.printStackTrace();
		}		
		String databaseAndTable = sqlCxn.getDatabase()+"."+tablename;
		String id, desc, query;
		id = idReader.readLine();
		desc = descReader.readLine();
		cxn = dbUtils.connectToMySql(url, username, password);
		java.sql.Statement stmt = cxn.createStatement();
		
		//clears table
		stmt.executeUpdate("delete from "+databaseAndTable+";");
		
		desc = formatVarchar(desc);
//		System.out.println(query);
		while(id != null && desc != null){
			desc = formatVarchar(desc);
			query = "insert into "+databaseAndTable+" (GameId,Description) values ("+id+",\""+desc+"\")";
			
			stmt.executeUpdate(query);
			id = idReader.readLine();
			desc = descReader.readLine();
		}
	}
	
	private String formatVarchar(String varchar){
//		varchar = String.format("%s", varchar.replaceAll("'", "''"));
		varchar = String.format("%s", varchar.replaceAll("\"", "'"));
		if(varchar.length() > 2000)
			varchar = varchar.substring(0, 2000);
		return varchar;
	}

}
