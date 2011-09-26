package com.ign.scrape.rating;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.jsoup.*;
import org.jsoup.nodes.Document;

import com.ign.utils;

public class RatingScraper {
	ArrayList <Long> gameIdsToScrape = new ArrayList <Long>();
	ArrayList <String> descriptions = new ArrayList <String> ();
	static String baseURL="http://itunes.apple.com/us/app/id";
	File inFile;
	
	/**@author ntomasino */
	
	public static void main(String... args) throws IOException, InterruptedException{
		final String help = "RatingScraper <gameIdFile> \n" +
				"gameIdFile - list of game id's to be scraped. separated by newLines \n" +
				"default itunes base url: " + baseURL;
		
		String infilePath = "";
		if(utils.checkArgs(args, 1, help)){
			infilePath = args[0];
		}
		
		RatingScraper gds = new RatingScraper();
		gds.loadIDs(infilePath);
		gds.scrapeGameIds();
	}
	
	private void loadIDs(String arg0) throws IOException{
		inFile = new File(arg0);
		BufferedReader idFile = new BufferedReader(new FileReader(inFile));
		String curLine = idFile.readLine();
		System.out.println("writing to file: " + arg0);
		int count=0;
		while(curLine!=null){
			try{
				gameIdsToScrape.add(Long.parseLong(curLine));
			} catch (NumberFormatException nfEx){
				System.out.println("unparsable game id at line: "+count);
			}
			curLine = idFile.readLine();
			++count;
		}
	}
	
	public void scrapeGameIds() throws IOException, InterruptedException{
//		String gameID="362949845"; //http://itunes.apple.com/us/app/id445853367
		Iterator <Long> gameIter = gameIdsToScrape.iterator();
		File outFile = new File(inFile.getPath()+".out");
		if(!outFile.exists())
			outFile.createNewFile();
		FileWriter fw = new FileWriter(outFile);
		String url, html;
		Document page, doc;
		
		while(gameIter.hasNext()){
			url = baseURL + gameIter.next();
			page = Jsoup.connect(url).get();		
			html = page.toString();
			doc = Jsoup.parse(html);
			
	//		<div more-text="More" metrics-loc="Titledbox_Description" class="product-review"> 
			String description = doc.getElementsByAttributeValue("metrics-loc", "Titledbox_Description").select("p").text();
			System.out.println(description);
			fw.write(description + "\n");
			Thread.sleep((long) (Math.random()*1000));
		}
		fw.close();
	}
	
}

/* Sample html snippet of where the game descriptions come from

</script> 
<div id="desktopContentBlockId" class="platform-content-block display-block"> 
 <div id="content"> 
  <div class="padder"> 
   <div id="title" class="intro "> 
    <h1>Rail Maze</h1> 
    <h2>By Spooky House Studios UG (haftungsbeschraenkt)</h2> 
    <a href="http://itunes.apple.com/us/artist/spooky-house-studios-ug-haftungsbeschraenkt/id376098163" class="view-more">View More By This Developer</a> 
    <p>Open iTunes to buy and download apps.</p> 
   </div> 
   <div class="center-stack"> 
    <!-- Description --> 
    <div more-text="More" metrics-loc="Titledbox_Description" class="product-review"> 
     <h4> Description </h4> 
     <p>Download the @FreeAppADay Store App and wish for more top rated apps like Rail Maze to become FREE for a day!&quot;<br /><br />Rail Maze is the latest game by Spooky House Studios - creators of big hits: Bubble Explode and Pumpkin Explode. <br /><br />Solve 100+ of challenging and unique puzzles, build railroads, bomb through obstacles, escape PIRATES on rails. Have a lot of fun with this new and unique puzzle game. <br /><br />Features: <br /><br />* 100+ puzzles <br />* Tunnels <br />* Bombs <br />* PIRATE trains <br />* Super long trains <br />* Global online scoreboards by Scoreloop <br />* 4 game modes: <br />LABYRINTH - Puzzle, <br />BUILD RAILROAD - Action, <br />SNAKE - Action, <br />LONGEST RAILROAD - Puzzle Action <br /><br />Get Rail Maze now!</p> 
    </div> 
    
*/