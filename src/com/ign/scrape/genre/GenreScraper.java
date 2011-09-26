package com.ign.scrape.genre;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.jsoup.*;
import org.jsoup.nodes.Document;

public class GenreScraper {
//	String basePath = "/home/ntomasino"; //should change this with every environment
	String basePath = ""; 
	ArrayList <Long> gameIdsToScrape = new ArrayList <Long>();
	ArrayList <String> descriptions = new ArrayList <String> ();
	String baseURL="http://itunes.apple.com/us/genre/";
	final static String [] genres = {
			"action",
			"adventure",
			"arcade",
			"board",
			"card",
			"casino",
			"dice",
			"educational",
			"family",
			"kids",
			"music",
			"puzzle",
			"racing",
			"role-playing",
			"simulation",
			"sports",
			"strategy",
			"trivia",
			"word"
	};
	String [] genreUrls =	{
			"http://itunes.apple.com/us/genre/ios-games-action/id7001?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-adventure/id7002?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-arcade/id7003?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-board/id7004?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-card/id7005?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-casino/id7006?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-dice/id7007?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-educational/id7008?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-family/id7009?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-kids/id7010?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-music/id7011?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-puzzle/id7012?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-racing/id7013?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-role-playing/id7014?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-simulation/id7015?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-sports/id7016?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-strategy/id7017?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-trivia/id7018?mt=8",
			"http://itunes.apple.com/us/genre/ios-games-word/id7019?mt=8"
	};
	char [] AZ = {'A','B','C','D','E','F','G','H','I','J','K','L','M','N',
			'O','P','Q','R','S','T','U','V','W','X','Y','Z','*'};
	char slash = File.separatorChar;
//			"?mt=8&letter=A&page=2#page";
	File inFile;
	
	public static void main(String... args) throws IOException, InterruptedException{
		if(args.length>0)
			if(args[0].trim().equalsIgnoreCase("--help"))
				System.out.println("\n GenreScraper downloads the genres for games into directory: /genreData/genres.tsv\n");

		GenreScraper genreScraper = new GenreScraper();
//		genreScraper.trialScrape();
		genreScraper.scrapeGenres();
	}
	
	public void trialScrape() throws IOException{
		for(int i=0; i < 19; i++){
		Document page = Jsoup.connect(genreUrls[i]+"&letter=A&page=101#page").get();		
		String html = page.toString();
		Document doc = Jsoup.parse(html);
		
		//		<div more-text="More" metrics-loc="Titledbox_Description" class="product-review">
		Object [] gameIds = doc.getElementsByAttributeValue("id", "selectedcontent").select("li").toArray();
		String gameId = doc.getElementsByAttributeValue("id", "selectedcontent").select("li").toString();
		
		if(gameIds.length < 5) // a bug exists in the apple website. single games claim to continue for hundreds of pages
			System.out.println(gameId);
		}
	}
	
	public void scrapeGenres() throws IOException, InterruptedException{
		Document page, doc = new Document("");
		String gameInfo, html, letterAndPage, finalUrl;
		Object [] gameInfoArray;
		File outFile;
		FileWriter fw = null;
		for(int genre=0; genre < 19; genre++){ //19
			outFile = new File(basePath+slash+"genreData"+slash+genres[genre]+".out");
			if(!outFile.exists()){
				outFile.getParentFile().mkdirs();
				outFile.createNewFile();
			}
			fw = new FileWriter(outFile);
			
			for(int letter = 0; letter < 27; letter++){ //27
				for(int pageNum = 1; true ; pageNum++){
					letterAndPage = "&letter=" + AZ[letter] + 
									"&page=" + pageNum + "#page";
//					page = Jsoup.connect(genreUrls[genre]+"&letter=A&page=101#page").get();
					finalUrl = genreUrls[genre]+letterAndPage;
					System.out.println(finalUrl);
					
					try{
						page = Jsoup.connect(finalUrl).timeout(10000).get();
					} catch (java.net.SocketTimeoutException e){
						System.out.println("\nSocketTimoutException thrown. \nItunes server let our request time out. \nSleeping for 5 seconds and then continuing...");
						Thread.sleep(5000);
						pageNum--;
						continue;
					}
					html = page.toString();
					doc = Jsoup.parse(html);
					
					//		<div more-text="More" metrics-loc="Titledbox_Description" class="product-review">
					gameInfoArray = doc.getElementsByAttributeValue("id", "selectedcontent").select("li").toArray();
					gameInfo = doc.getElementsByAttributeValue("id", "selectedcontent").select("li").toString();
//					System.out.println(gameInfo);
					fw.write(gameInfo);
	
					// a bug exists in the apple website. single games claim to continue for hundreds of pages
					if(gameInfoArray.length < 5){
						break;
					}
//					Thread.sleep((long) (Math.random()*1000));
				}
				Thread.sleep((long) (Math.random()*1000));
			}
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