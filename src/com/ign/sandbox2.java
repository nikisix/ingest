package com.ign;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class sandbox2 {
	public static void main(String [] args) throws IOException{
		File f = new File("./test.txt");
		BufferedReader br = new BufferedReader(new FileReader(f));
		System.out.println(br.readLine());

//		File f = new File("./");
//		System.out.println(f.getAbsolutePath());
//		System.out.println(f.getPath());
	}
}
