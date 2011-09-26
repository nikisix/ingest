package com.ign.liveFeed;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

import com.ign.csv.CSVWriter;

/**
 * Basically this class reads in the tsv that itunes publishes and writes a csv class out.
 * Why go from tsv -> csv ?
 * Writing the file first ensures CSVToGamesTable will be able to read in the file without error.
 **/
public class ItunesFeedToCSV
{
	private static String Genres = "Games_"; // leave the trailing underscore!
	
	public static void main(String[] args) throws Exception
	{
		
		if (args.length != 2)
		{
			System.out.println("Reads from raw itunes feed file (tab-delimited) and writes to csv output file.");
			System.out.println("The resulting file is ready for import to MySql ingest database");
			System.out.println("Usage: IngestItunesFeed rawItunesFeedFile outFile");
			System.out.println("example: IngestItunesFeed application-usa.tsv application-usa.csv");
			System.exit(1);
		}
		
		BufferedReader reader = new BufferedReader(new FileReader(args[0]));
		CSVWriter writer = new CSVWriter(new FileWriter(args[1]));
		String[] fields = null;
		String[] modFields = null;
		
		int LineCount = 0;
		while (true)
		{
			String l = reader.readLine();
			if (l == null)
			{	
				reader.close();
				break;
			}

			// Maintain an accurate line count for error/warning messages
			++LineCount;

			// Skip comment rows
			if (l.charAt(0) == '#')
			{
				continue;
			}

			// Split the fields by the tab delimiter
			fields = l.split("\t");

			// There had better be 19 fields for each line
			if (fields.length < 18)
			{
				System.out.format("Unexpected tab-delimited field count at row %d\n", LineCount);
				continue;
			}
						
			// Skip any non-game rows
			String genre = fields[3] + "_";
			if (Genres.indexOf(genre) == -1)
			{
				continue;
			}

			// Create an array with room for the extracted game id
			modFields = new String[20];

			// Copy the field values into our larger array
			for (int i=0; i<modFields.length; ++i)
			{
				if (i >= fields.length){  //incoming fields are of varying length.
					modFields[i] = "";
				}else if (fields.length < 1 || fields[i].equals("") || fields[i] == null ){  //handle empty fields correctly. String.charAt starts from index-1
					modFields[i] = ""; 
				}
				else { // if a field ends with a '\' , then take of the trailing slash as it will cause unwanted escaping downstream 
					modFields[i] = fields[i].charAt(fields[i].length()-1) != '\\' ? fields[i] : fields[i].substring(0, fields[i].length()-1);
				}
			}
			
			// Extract the game id
			String[] parts = fields[4].split("[?]");
			if (parts.length != 2)
			{
				System.out.format("Unable to split game id from url at row %d\n", LineCount);
			}
			
			//Set the apple game ID as field 19
			modFields[19] = parts[0].substring(parts[0].length() - 9);
			
			writer.writeNext(modFields);
		}
		reader.close();
		writer.close();
		System.exit(0);
	}
}