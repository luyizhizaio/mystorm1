package com.lcy.slidewindow.utils;

import java.util.Random;

import backtype.storm.tuple.Values;

public class Main {
	
	private String[] data ={"222.36.188.206	20130530235959	uc_server/avatar.php?uid=57279&size=middle HTTP/1.1",
			"2.36.188.206	20150530235349	uc_server/reg.php? HTTP/1.1",
			"2.36.188.201	20150530235229	uc_server/abc.php?uid=57279&size=middle HTTP/1.1",
			"10.36.18.2	20130530235959	uc_server/dddd.php?uid=57279&size=middle HTTP/1.1"}; 

	
	public static void main(String[] args) {
		Random random = new Random();
		for (int i = 0; i < 50; i++) {
			int nextInt = random.nextInt(3);
			
			System.out.println(nextInt);
		}
		
	}

}
