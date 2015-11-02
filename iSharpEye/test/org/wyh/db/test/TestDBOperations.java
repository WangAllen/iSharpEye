package org.wyh.db.test;

import org.wyh.db.utils.DBOperations;

public class TestDBOperations {

	public static void testSearchFriends() {
		int idToFind = 45;
		DBOperations.searchFriends(idToFind);
	}

	public static void main(String[] args) {
		new DBOperations("my/graphDB");
		System.out.printf("total users: %d\t\ntotal pois: %d\t\n", DBOperations.totalUsers(), DBOperations.totalPOIs());
		testSearchFriends();
	}

}
