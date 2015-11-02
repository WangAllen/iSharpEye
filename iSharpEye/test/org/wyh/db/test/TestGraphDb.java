package org.wyh.db.test;

import org.wyh.db.utils.CreateDB;

public class TestGraphDb {

	public static void testCreateGraphDb() {
		new CreateDB("my/graphDb");
		System.out.println("Test over!");
	}
	
	public static void main(String[] args) {
		testCreateGraphDb();

	}

}
