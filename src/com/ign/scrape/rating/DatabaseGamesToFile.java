package com.ign.scrape.rating;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.ign.utils;
import com.ign.db.MySqlConnection;
import com.ign.db.dbUtils;

public class DatabaseGamesToFile {

	/** @author ntomasino
	 * 
	 * Pulls from the ingestion games table, all of the games published today. Their ids will be written to a file.
	 * One id per line. This file will be consumed by the GameDescriptionScraper and RatingScraper
	 * 
	 * Arguments may be supplied on the command line, otherwise defaults will be used
	 * @param args - output file path
	 * @throws Exception 
	 */
	
	final static String n = "\n";
	
	public static void main(String[] args) throws Exception {
		String url = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
		String username = "mdadmin";
		String password = "l3m0n";
		String tablename = "gamesBuffer";
		String columnName = "GameId";
		MySqlConnection myCxn = new MySqlConnection(url, username, password, tablename);
		
		final String help = "DatabaseGamesToFile takes the name of a file path to print out the new games from the games database\n" +
				"DatabaseGamesToFile <output_filePath> \n" +
				"DatabaseGamesToFile <ouput_filePath> <databaseUrl> <username> <password> <tablename>\n" +
				"defaults: \n" +
				"url: jdbc:mysql://10.92.217.17:3306/ingest_staging \n" +
				"username: mdadmin \n" +
				"password: l3m0n \n" +
				"tablename: gamesBuffer \n" +
				"columnName: GameId \n";
		String outfilepath = "";
		
		if (com.ign.utils.checkArgs(args, 1, help))
			outfilepath = args[0]; //brace	
		else if (utils.checkArgs(args, 6, help)){
			outfilepath = args[0];
			url = args[1];
			username = args[2];
			password = args[3];
			tablename = args[4];
			columnName = args[5];
		} else {
			System.out.println(help);
			return;
		}
			
		
		System.out.println("DatabaseGamesToFile running with the following parameters... " +n+
								"output file path: "+ outfilepath + n+ 
								myCxn.toString()
							);
		
		writeNewGamesFile(myCxn, outfilepath, tablename, columnName);
	}
	
	public static void writeNewGamesFile(MySqlConnection myCxn, String outfilepath, String tablename, String columnName) throws Exception{
		File outFile = new File(outfilepath);
		if(!outFile.exists())
			outFile.createNewFile();
		FileWriter fw = new FileWriter(outFile); 
		Connection cxn = dbUtils.connectToMySql(myCxn.getUrl(), myCxn.getUsername(), myCxn.getPassword());
		
		java.sql.Statement stmt = cxn.createStatement();
//		String query = "select GameId from gamesBuffer where ReleaseDate = '"+date+"'";
		String query = "select "+columnName+" from "+tablename;
		ResultSet rs = stmt.executeQuery(query); //maybe executeUpdate
		printResultSetToFile(rs,fw);
		stmt.close();
		fw.close();
	}
	
	public static void printResultSetToFile(ResultSet rs, FileWriter fw) throws SQLException, IOException{
		while(rs.next()){
//			System.out.println(rs.getString("GameId"));
			fw.write(rs.getString("GameId")+n);
		}
	}
}
