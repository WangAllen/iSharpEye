package org.wyh.db.test;

import java.util.ArrayList;

import org.wyh.db.utils.DBOperations;

public class TestDBOperations {

	public static void getNumbers() {
		int totalUsers = DBOperations.totalUsers();
		int totalPOIs = DBOperations.totalPOIs();
		System.out.printf("total users: %d\t\ntotal POIs: %d\t\n", totalUsers, totalPOIs);
	}

	public static void testSearchFriends() {
		int idToFind = 45;
		DBOperations.searchFriends(idToFind);
	}

	public static void test_getVisitedPOIs() {
		int idToFind = 45;
		ArrayList<String> pois = DBOperations.getVisitedPOIs(idToFind);
		int total = 0;
		for (String p : pois) {
			System.out.print(p + "\t");
			total++;
			if (total % 3 == 0)
				System.out.println();
		}
	}

	public static void test_getVisitedPOIsByTimePeriod() {
		int idToFind = 45;
		String startTime = "2009-03-23 ";
		String endTime = "2010-01-01";
		ArrayList<String> pois = DBOperations.getVisitedPOIsByTimePeriod(idToFind, startTime, endTime);
		int total = 0;
		for (String p : pois) {
			System.out.print(p + "\t");
			total++;
			if (total % 3 == 0)
				System.out.println();
		}
	}

	public static void test_getVisitedPOIsByArea() {
		int idToFind = 45;
		double lat1 = 35;
		double lon1 = -110;
		double lat2 = 50;
		double lon2 = -90;
		ArrayList<String> pois = DBOperations.getVisitedPOIsByArea(idToFind, lat1, lat2, lon1, lon2);
		int total = 0;
		for (String p : pois) {
			System.out.print(p + "\t");
			total++;
			if (total % 3 == 0)
				System.out.println();
		}
	}

	public static void test_getVisitors() {
		String poiId = "ee8b1d0ea22411ddb074dbd65f1665cf";
		ArrayList<Integer> visitors = DBOperations.getVisitors(poiId);
		int total = 0;
		for (Integer u : visitors) {
			System.out.print(u + "\t");
			total++;
			if (total % 3 == 0)
				System.out.println();
		}
	}
	
	
	public static void test_getVisitorsByTimePeriod() {
		String poiId = "ee8b1d0ea22411ddb074dbd65f1665cf";
		String startTime = "2009-03-23 ";
		String endTime = "2010-01-01";
		ArrayList<Integer> visitors = DBOperations.getVisitorsByTimePeriod(poiId, startTime, endTime);
		int total = 0;
		for (Integer u : visitors) {
			System.out.print(u + "\t");
			total++;
			if (total % 3 == 0)
				System.out.println();
		}
	}
	
	public static void main(String[] args) {
		new DBOperations("my/graphDB");
//		System.out.printf("total users: %d\t\ntotal pois: %d\t\n", DBOperations.totalUsers(), DBOperations.totalPOIs());
		// test_getVisitedPOIs();
//		test_getVisitedPOIsByTimePeriod();
//		test_getVisitedPOIsByArea();
//		test_getVisitors();
		test_getVisitorsByTimePeriod();
		// testSearchFriends();
		// getNumbers();
	}

}
