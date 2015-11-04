package org.wyh.db.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestHelloworld {

	public static void main(String[] args) {
		System.out.println("Hello World!");
		String str1 = "2012-12-06 11:30:20";
		String str2 = "2013-12-06 11:30:20";
		String str3 = "2013-12-06 11:30:19";
		System.out.println("str1 compares to str1 " + str1.compareToIgnoreCase(str1));
		System.out.println("str1 compares to str2 " + str1.compareToIgnoreCase(str2));
		System.out.println("str2 compares to str3 " + str2.compareToIgnoreCase(str3));
//		String dateString = "2012-12-06 11:30:20";
//		try {
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//			Date date = sdf.parse(dateString);
//
//			System.out.println(date);
//		} catch (ParseException e) {
//			System.out.println(e.getMessage());
//		}
	}

}
