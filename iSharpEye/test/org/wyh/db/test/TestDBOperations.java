package org.wyh.db.test;

import java.util.ArrayList;

import org.wyh.db.utils.CalcUtils;
import org.wyh.db.utils.DBOperations;

public class TestDBOperations {

	/**
	 * 获取数据库节点数：用户，兴趣点
	 */
	public static void getNumbers() {
		int totalUsers = DBOperations.totalUsers();
		int totalPOIs = DBOperations.totalPOIs();
		System.out.printf("total users: %d\t\ntotal POIs: %d\t\n", totalUsers, totalPOIs);
	}

	/**
	 * 查找用户好友
	 */
	public static void test_getFriends() {
		int idToFind = 45;
		ArrayList<Integer> friends = DBOperations.getFriends(idToFind);
		for (int i = 0; i < friends.size(); i++) {
			System.out.print(friends.get(i) + "\t");
			if ((i+1)%10 == 0) 
				System.out.println();
		}
	}

	/**
	 * 查找用户访问过的POI
	 */
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

	/**
	 * 查询用户某段时间内访问过的POI
	 */
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

	/**
	 * 查找用户在某个区域内访问过的POI
	 */
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

	/**
	 * 查找某个POI的游客
	 */
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
	
	
	/**
	 * 查找POI在某段时间内的游客
	 */
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
	
	/**
	 * 查询POI被访问的次数
	 */
	public static void test_getVisitedTimes() {
		String poiId = "ee8b1d0ea22411ddb074dbd65f1665cf";
		int times = DBOperations.getVisitedTimes(poiId);
		System.out.printf("poi %s is visited by %d times.\n", poiId, times);
	}
	
	
	public static void test_getPOIHotTime() {
		String poiId = "ee8b1d0ea22411ddb074dbd65f1665cf";
		int[] times = DBOperations.getPOIHotTime(poiId);
		for (int i=0; i <times.length;i++) {
			System.out.printf("time slot: %d and visited times: %d \n", i, times[i]);
		}
	}
	
	public static void test_getPOIHotTimes() {
		ArrayList<String> pois = DBOperations.getPOIs();
		int count = 50;
		long[] randnums = CalcUtils.randomNumbers(pois.size(), count);
		for (int i=0; i<count; i++) {
			System.out.println("randomNumber " + i + " = " + randnums[i]);
			String poiId = pois.get((int) randnums[i]);
			System.out.println(poiId);
			int[] times = DBOperations.getPOIHotTime(poiId);
			for (int j=0; j <times.length;j++) {
//				System.out.printf("time slot: %d and visited times: %d \n", j, times[j]);
				System.out.print(" " + times[j]);
			}
			System.out.println("\n#################################################");
		}
		
	}
	
	public static void main(String[] args) {
		new DBOperations("my/graphDB");
//		getNumbers();	//	获取数据库总节点数
//		testSearchFriends();	//	用户好友查询
//		test_getVisitedPOIsByTimePeriod();	//	用户某段时间签到历史
//		System.out.printf("total users: %d\t\ntotal pois: %d\t\n", DBOperations.totalUsers(), DBOperations.totalPOIs());
		// test_getVisitedPOIs();
//		
//		test_getVisitedPOIsByArea();
//		test_getVisitors();
//		test_getVisitorsByTimePeriod();
		
		// getNumbers();
//		test_getVisitedTimes();
//		test_getPOIHotTime();
		test_getPOIHotTimes();
	}

}
