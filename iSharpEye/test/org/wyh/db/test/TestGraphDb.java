package org.wyh.db.test;

import org.wyh.db.utils.CreateDB;
import org.wyh.db.utils.DatabaseUtils;

public class TestGraphDb {

	public static void testCreateGraphDb() {
		new CreateDB("my/graphDb");
		System.out.println("Test over!");
	}
	
	public static void main(String[] args) {
		testCreateGraphDb();
//		DatabaseUtils.clearDb("my/graphDb");
	}

}
