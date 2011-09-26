package com.ign.scrape.genre;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;

import com.ign.db.MySqlConnection;
import com.ign.db.dbUtils;

import org.jsoup.*;
import org.jsoup.nodes.Document;

public class GenreFilesToDatabase {

	/**@author ntomasino
	 * @param String gameIdFile
	 * @param String gameDescriptionFile
	 */
	
	String baseGenrePath;
	String url, username, password, tablename;
	MySqlConnection sqlCxn;
	Connection cxn;
	
	final static char slash = File.separatorChar;
	final static String [] genres = GenreScraper.genres;
	
	public static void main(String[] args) throws Exception {
		final String help = "GenreFilesToDatabase (if no parameters specified, defaults will be used) \n" +
				"GenreFilesToDatabase <baseGenrePath> <databaseUrl> <dbUsername> <dbPassword> <tablename> \n" +
				"<baseGenrePath> - default: /genreData/. \n" +
				"<databaseUrl> - default: jdbc:mysql://10.92.217.17:3306/ingest_staging \n" +
				"<tablename> - default: genreBuffer \n";
		
		GenreFilesToDatabase dfdb = new GenreFilesToDatabase();
		
		if (com.ign.utils.checkArgs(args, 0, help)){
			System.out.println("Using defaults for databaseUrl, username, password, and tablename");
			dfdb.baseGenrePath = "/genreData/"; 
			dfdb.url = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
			dfdb.username = "mdadmin";
			dfdb.password = "l3m0n";
			dfdb.tablename = "genreBuffer";
		} else if (com.ign.utils.checkArgs(args, 5, help)) {
			dfdb.baseGenrePath = args[0]; 
			dfdb.url = args[1];
			dfdb.username = args[2];
			dfdb.password = args[3];
			dfdb.tablename = args[4];
		} else {
			System.err.println("Wrong number of arguments\n" + help);
			return;
		}
				
		dfdb.sqlCxn = new MySqlConnection(dfdb.url, dfdb.username, dfdb.password, dfdb.tablename);

//		dfdb.loadGameIdsAndDescriptions();
		dfdb.writeToDatabase();
	}
		
	/**create database connection
	 * for each genre.out 
	 * 		for each line
	 * 			parse gameId
	 * 			insert database.table (GameId,Genre) values (id,curGenre) 
	 *  
	 * @throws Exception*/
	
		public void writeToDatabase() throws Exception{
		BufferedReader reader = null;
		File inFile = null;
		String databaseAndTable = sqlCxn.getDatabase()+"."+tablename;
		String curLine, curGenre, query;
		long gameId;
		
		cxn = dbUtils.connectToMySql(url, username, password);
		java.sql.Statement stmt = cxn.createStatement();
		//clears table
		stmt.executeUpdate("delete from "+databaseAndTable+";");
		
		for(int genre=0; genre < 19; genre++){ //19
			curGenre = genres[genre];
			inFile = new File(baseGenrePath+slash+curGenre+".out");
		
			try {
				reader = new BufferedReader(new FileReader(inFile));
			} catch (FileNotFoundException e) {
				System.err.println("GenreFilesToDatabase.java: Couldn't read in file: " + inFile);
				continue;
			}
			
			curLine = reader.readLine();
		
			while(curLine != null){
				try{
					gameId = parseGameId(curLine, "/id", "?mt=8");
					query = "insert into "+databaseAndTable+" (GameId,Genre) values ("+gameId+",\""+curGenre+"\")";
					System.out.println(query);
				} catch (java.lang.NumberFormatException nfe){
					System.err.println("Number Format Exception on line: " + curLine + " In file: " + genres[genre]);
					curLine = reader.readLine();
					continue;
				}
				stmt.executeUpdate(query);
				curLine = reader.readLine();
			}
		}
	}
	
	private Long parseGameId(String line, String startPattern, String stopPattern){
		//fails on /idropples/id354033846/?mt=8">iDropples
		String [] tokens = line.split(startPattern);
		String id ="", escape = "\\";
		
		for (String token : tokens){
			if(token.contains(stopPattern)){
				id = token.split(escape+stopPattern)[0];
			}
		}
//		int idStart = line.indexOf(startPattern);  
//		int idEnd = line.indexOf(stopPattern);
//		System.out.println("start = " + idStart + "stop = " + idEnd + " " + line);
//		return Long.parseLong(line.substring(idStart + startPattern.length(), idEnd));
//		System.out.println("id: "+id);
		return Long.parseLong(id);
	}

}



















































