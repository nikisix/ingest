package com.ign;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class utils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(getMySqlDate());
	}

	public static boolean checkArgs(String [] args, int numArgs, String help){
		if(args.length == 0){
			if(numArgs == 0) return true;
			else return false;
		}
		if(args.length!=numArgs){
			System.out.println(help);
			return false;
		}
		if(args[0].trim().equalsIgnoreCase("--help") || args[0].trim().equalsIgnoreCase("-help") 
		|| args[0].trim().equalsIgnoreCase("-?") || args[0].trim().equalsIgnoreCase("-h") ) {
			System.out.println("help");
			return false;
		}
		return true;
	}
	
	
	public static String parseDate(String field) throws ParseException{
		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy MM dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(inputDateFormat.parse(field));
	}
	
	public static String getMySqlDate(){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar now = Calendar.getInstance();
		return dateFormat.format(now.getTime());
	}
	
	public static FileWriter outFileWriterFromInFile(File inFile) throws IOException{
		File outFile = new File(inFile.getPath()+".out");
		if(!outFile.exists())
			outFile.createNewFile();
		FileWriter fw = new FileWriter(outFile);
		return fw;
	}
	
	public static File outFileFromInFile(File inFile) throws IOException{
		File outFile = new File(inFile.getPath()+".out");
		if(!outFile.exists())
			outFile.createNewFile();
		return outFile;
	}
}
