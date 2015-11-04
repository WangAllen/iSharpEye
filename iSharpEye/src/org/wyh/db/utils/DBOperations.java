package org.wyh.db.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.neo4j.cypher.internal.compiler.v2_2.ast.In;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class DBOperations {
	private static GraphDatabaseService graphDb;
	
	public DBOperations (String path) {
		graphDb = DatabaseUtils.getDbService(path);
	}
	
	public static int totalUsers() {
		int total = 0;
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> users = graphDb.findNodes(MyLabels.USER);
			while (users.hasNext()) {
				users.next();
				total++;
			}
			tx.success();
		}
		return total;
	}
	
	public static int totalPOIs() {
		int total = 0;
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> pois = graphDb.findNodes(MyLabels.POI);
			while (pois.hasNext()) {
				pois.next();
				total++;
			}
			tx.success();
		}
		return total;
	}
	
	public static Node searchUser(int userId) {
		Node userNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			tx.success();
		}
		return userNode;
	}
	
	public static Node searchPOI(String poiId) {
		Node poiNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			tx.success();
		}
		return poiNode;
	}
	
	public static void searchFriends(int userId) {
		try (Transaction tx = graphDb.beginTx()) {
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);			
			System.out.printf("\nDegree of user %d is: %d\n", userId, userNode.getDegree());
			Iterable<Relationship> rs = userNode.getRelationships(RelTypes.IS_FRIEND_OF);
			int total = 0;
			for (Relationship r : rs) {
				Node friendNode = r.getOtherNode(userNode);
				System.out.print(friendNode.getProperty(Consts.userId_key) + "\t");
				total++;
				if (total%10 == 0)
					System.out.println();
			}
			
			tx.success();
		}
	}
	
	public static void preprocess() {
		
	}
	
	/**
	 * 查找一个用户访问过的所有兴趣点
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIs(int userId) {
		// 兴趣点列表
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 获取目标用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) {	// 用户非空
				// 获取用户的签到记录
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its ) {
					// 获取签到的兴趣点
					Node poiNode = r.getEndNode();
					// 兴趣点添加到访问列表
					visitedPOIs.add((String) poiNode.getProperty(Consts.poiId_key));
				}
			}
			// 提交
			tx.success();
		}
		return visitedPOIs;
	}
	
	
	/**
	 * 查询一段时间内，用户访问过的位置
	 * @param userId int
	 * @param startTime String
	 * @param endTime String
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIsByTimePeriod(int userId, String startTime, String endTime){
		// 已访问过的兴趣点列表
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 获取用户节点
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null ){	// 用户节点不为空
				/*
				 * 查找到用户所有的签到记录，然后从中找出签到时间在规定范围内的签到记录
				 */
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);	// 查找用户的所有签到记录
				for (Relationship r : its ) {	// 根据签到记录的时间属性，比较起讫时间
					String[] timeArray = (String[]) r.getProperty(Consts.tm);	// 签到时间数组
					for (String time : timeArray) {	// 逐一比较
						// 只要有一个符合要求，即可结束，并将对应兴趣点记录到列表
						if (time.compareToIgnoreCase(startTime) > 0 && time.compareToIgnoreCase(endTime) < 0) {
							visitedPOIs.add((String) r.getEndNode().getProperty(Consts.poiId_key));
							// 只要有一个符合要求，即返回
							break;
						}
					}
				}
			} else {
				System.out.println("userId " + userId + " does not exist in the database.");
			}
			tx.success();
		}
		
		return visitedPOIs;
	}
	
	/**
	 * 根据经纬度范围查找POI
	 * @param userId
	 * @param latitude1
	 * @param latitude2
	 * @param longitude1
	 * @param longitude2
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIsByArea(int userId, double latitude1, double latitude2, double longitude1, double longitude2) {
		// 查找区域范围内兴趣点
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始
		try (Transaction tx = graphDb.beginTx()) {
			//　获取用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null ){
				// 找到用户的所有签到关系
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				// 遍历
				for (Relationship r : its ) {
					//找到POI
					Node poiNode = r.getEndNode();
					// 获取POI经纬度
					double[] loc = (double[]) poiNode.getProperty(Consts.loc);
					// 经纬度在范围内
					if (loc[0] > latitude1 && loc[0] <= latitude2 && loc[1] > longitude1 && loc[1] <= longitude2) {
						// 添加到列表
						visitedPOIs.add((String) poiNode.getProperty(Consts.poiId_key));
					}
				}
			}
			tx.success();
		}
		return visitedPOIs;
	}
	
	/**
	 * 查找访问过该POI的游客
	 * @param poiId
	 * @return
	 */
	public static ArrayList<Integer> getVisitors(String poiId) {
		// 用户列表
		ArrayList<Integer> visitors = new ArrayList<Integer>();
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 目标兴趣点
			Node poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			if (poiNode != null) {// 非空？
				// 查找该POI的访问记录
				Iterable<Relationship> its = poiNode.getRelationships(RelTypes.CHECKIN);
				// 遍历
				for (Relationship r : its) {
					// 获取用户
					Node userNode = r.getStartNode();
					// 获取用户id
					int userId = (int) userNode.getProperty(Consts.userId_key);
					// 添加到用户列表
					visitors.add(userId);
				}
			}
			tx.success();
		}
		return visitors;
	}
	
	/**
	 * 查找某段时间内（startTime, endTime）访问过该POI的游客
	 * @param poiId
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static ArrayList<Integer> getVisitorsByTimePeriod(String poiId, String startTime, String endTime) {
		// 游客列表
		ArrayList<Integer> visitors = new ArrayList<Integer>();
		// 开始事务：查找
		try (Transaction tx = graphDb.beginTx()) {
			// 获取POI节点
			Node poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			if (poiNode != null ) {	// 非空
				// 查找该POI的签到记录
				Iterable<Relationship> its = poiNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) {	// 遍历
					// 签到属性：时间
					String[] timeArray = (String[]) r.getProperty(Consts.tm);
					for (String time : timeArray) {
						// 判断是否在时间区间内
						if (time.compareToIgnoreCase(startTime) > 0 && time.compareToIgnoreCase(endTime) < 0) {
							// 符合要求，记录用户
							Node userNode = r.getStartNode();	// 用户节点
							int userId = (int) userNode.getProperty(Consts.userId_key);	// 用户Id
							visitors.add(userId);// 加入用户列表
							break;
						}							
					}					
				}
			}
			tx.success();
		}
		return visitors;
	}
}
