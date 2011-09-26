package com.ign.liveFeed;
/**
 * Project includes: CSVWriter.java, CSVReader.java, ConvertItunesFeed.java, feedFileToTable.java
 * 
 * wget http://share.ign.com/ign/feeds/iphone/application-usa.tsv --user product --password tasty\ xml 
 * ConvertItunesFeed(application-usa.tsv) -> application-usa.csv
 * feedFileToTable(application-usa.csv) -> updates the ingest_staging.itunesGames_20110808_1 database
 * */

import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.ign.csv.CSVReader;

public class CSVToGamesTable  {
/**@author ntomasino	 
 * these are the fields as they exist in the live feed document.
 * 
	final String insertStatementHead = "INSERT INTO ingest_staging.itunesGames_20110808_1 "+
	"(Title, DeveloperName, SellerName, PrimaryGenreName, ApplicationUrl, CompanyUrl, SupportUrl, LargeIconUrl, SmallIconUrl, " +
	"ScreenShotUrl, ContentRating, Version, ItunesVersion, ReleaseDate, Price, DownloadSize, Copyright, SupportedDevices, " +
	"FirstIpadScreenshotUrl, GameId) VALUES ("; 
	VALUES ('Title', 'DeveloperName', 'SellerName', 'PrimaryGenreName', 'ApplicationUrl', 'CompanyUrl', 'SupportUrl', 'LargeIconUrl', 
	'SmallIconUrl', 'ScreenShotUrl', 'ContentRating', 'Version', 'ItunesVersion', 'ReleaseDate', Price, DownloadSize, 'Copyright', 
	'SupportedDevices', 'FirstIpadScreenshotUrl', GameId);
	
*/
	
//	final static String userid="mdadmin", password = "l3m0n";//, databaseAndTable = "ingest_staging.itunesGames_20110808_1";
	static String n = "\n";
	static java.sql.Statement statement;
	static Connection connection;
	static String help = 
		"CSVToGamesTable Reads from reformatted itunes feed file (csv-delimited), and imports to a MySql database.  \n"+
		"Usage: CSVToGamesTable /path/to/liveFeedFile.csv databaseName.templateTable servername:portNumber/databaseName userName password"+n+n+
		"if no arguments are supplied, the following default parameters will be used: "+n+
		"./application-usa.csv ingest_staging.itunesGames_template ingest_staging.gamesBuffer jdbc:mysql://10.92.217.17:3306/ingest_staging mdadmin l3m0n"+n;
//	/home/ntomasino/itunesData/sept6/application-usa.csv ingest_staging.itunesGames 10.92.217.17:3306/ingest_staging:user=mdadmin;password=l3m0n
//	./application-usa.csv ingest_staging.itunesGames_template ingest_staging.gamesBuffer 10.92.217.17:3306/ingest_staging mdadmin l3m0n
	
	// String url = “jdbc:mySubprotocol:myDataSource”; 

	//takes in a live itunes feed file and writes out to a mysql database
	public static void main(String [] args) throws Exception{
		String liveFeedPath = "", templateTable ="", targetTable="", url="", username="", password = "";
		if(args.length==0)	{
			//defaults 
			liveFeedPath = "./application-usa.csv"; // for production
//			liveFeedPath = "/home/ntomasino/itunesData/sept6/application-usa.csv"; //testing 
			templateTable = "ingest_staging.itunesGames_template";
			targetTable = "ingest_staging.gamesBuffer";
			url = "jdbc:mysql://10.92.217.17:3306/ingest_staging";
			username = "mdadmin";
			password = "l3m0n";
		} else if (args.length==6 ) {
			//use args
			liveFeedPath = args[0];
			templateTable = args[1];
			targetTable = args[2];
			url = args[3];
			username = args[4];
			password = args[5];
		} else {
	 		System.out.println("Wrong number of arguments \n"+help);
	 		System.exit(1);
	 	}
//		String databaseAndTable = "ingest_staging.itunesGames_20110906";
//		String liveFeedPath = "/home/ntomasino/itunesData/sept6/application-usa.csv";

//		url = "jdbc:mysql://" + url;
		if(checkArgs(liveFeedPath, templateTable, targetTable, url)){
			System.out.println("CSVToGamesTable running with the following parameters: \n " +
					"live feed path: " + liveFeedPath + n + 
					"templateTable: " + templateTable + n +
					"targetTable: " + targetTable + n +
					"database url: " + url + n +
					"username: " + username + n +
					"password: " + password		
			);
			CreateTable(url, username, password, templateTable, targetTable );
			ingestGames(liveFeedPath, targetTable, url, username, password);
		}else{
			System.err.print("Arguments not of correct format \n" + help +n+n+
					"arguments recieved: "+n+ liveFeedPath +n+ templateTable +n+ targetTable +n+ url +n+ username +n+ password);
		}
	}
	
	private static boolean checkArgs(String feedPath, String templateTable, String targetTable, String url){
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
	
	private static boolean CreateTable(String url, String user, String password, String templateTable, String targetTable) throws ClassNotFoundException, SQLException{
		String statamento = "create table "+targetTable+" as select * from "+templateTable+" limit 0";
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
	
	private static Connection OpenMySQL(String url, String user, String password) throws Exception {
        Connection c = null;
        // Load the JDBC driver
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);

        // Make the connection to the database
        c = DriverManager.getConnection(url,user,password);
          return c;
    }

	
	static String getHeader(String [] columns, String databaseAndTable){
		String head = "INSERT INTO "+ databaseAndTable + " (";
		for(String col : columns){
			head += col+", ";
		}
		return head.substring(0, head.length()-2) + ") Values (";
	}
	
	/**
	 * feeds the ingest_staging.itunesGames_<date>_<version> table
	 * to be cron'd
	 * @param liveFeedFile is the itunesLiveFeedFile converted to csv form by ConvertItunesFeed.java, which uses CSVWriter.java
	 * */
	static void ingestGames(String liveFeedPath, String databaseAndTable, String url, String user, String password) throws Exception{
		
		Connection connection = OpenMySQL(url, user, password);
		statement = connection.createStatement();
		statement.setQueryTimeout(0);
		statement.setFetchSize(0);
		statement.executeUpdate("delete from "+databaseAndTable+";");
		long counter =0;
		//automateDate
		final String [] Columns = {
				"Title",
				"DeveloperName",
				"SellerName",
				"PrimaryGenreName",
				"ApplicationUrl",
				"CompanyUrl",
				"SupportUrl",
				"LargeIconUrl",
				"SmallIconUrl",
				"ScreenShotUrl",
				"ContentRating",
				"Version",
				"ItunesVersion",
				"SupportedDevices",
				"Price",
				"ReleaseDate",
				"Copyright",
				"DownloadSize",
				"FirstIpadScreenshotUrl",
				"GameId",
		};
		
		final String [] Types = {
				"VARCHAR",
				"VARCHAR",
				"VARCHAR",
				"VARCHAR",
				"VARCHAR",
				"VARCHAR",  
				"VARCHAR",   
				"VARCHAR",   
				"VARCHAR",
				"VARCHAR",   
				"VARCHAR",    
				"VARCHAR",    
				"VARCHAR",    
				"VARCHAR", //TEXT really    
				"DECIMAL",    
				"DATETIME",        
				"VARCHAR",   
				"BIGINT",  
				"VARCHAR",
				"INT",
		};

		String insertStatement = "";
		CSVReader reader = new CSVReader (new FileReader (new File(liveFeedPath)));
		int curColumn = 0;
		
			String [] fields = reader.readNext();
			while(fields != null){
				insertStatement = getHeader(Columns, databaseAndTable);
				curColumn=0;
				
				for(String field : fields){
					if(Types[curColumn].equalsIgnoreCase("DATETIME")) //yyyy MM dd -> yyyy-MM-dd
					{
						field = parseDate(field);
						field = "'"+field+"'";
					}else if (Types[curColumn].equalsIgnoreCase("VARCHAR")){
						field = String.format("'%s'", field.replaceAll("'", "''"));
					}

					insertStatement += field+",";
					curColumn++;
				}
				//take off last comma and append the closing paren and semicolon for the mysql statement
				insertStatement = insertStatement.substring(0, insertStatement.length()-1) + ");";
//				System.out.println(insertStatement);
				try {
					statement.executeUpdate(insertStatement);
				} catch(Exception ex){
					System.err.println("Ingest Games Exception: " + ex.getMessage() + "\n insertStatement = "+insertStatement+ "\n count = " + counter);
				}
				if(++counter%10000==0){System.out.println(counter+" lines written");}
				fields = reader.readNext();
			}
			statement.close();
			connection.close();
		
	}

	public static boolean isDate(String field){
//		TODO
		return false;
	}
	
	public static String parseDate(String field) throws ParseException{
		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy MM dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(inputDateFormat.parse(field));
	}
}