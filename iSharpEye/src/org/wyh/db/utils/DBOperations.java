package org.wyh.db.utils;

import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class DBOperations {
	private static GraphDatabaseService graphDb;

	public DBOperations(String path) {
		graphDb = DatabaseUtils.getDbService(path);
	}

	/**
	 * 用户总数
	 * @return
	 */
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

	/**
	 * POI总数
	 * @return
	 */
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

	/**
	 * 查询用户
	 * @param userId
	 * @return
	 */
	public static Node getUser(int userId) {
		Node userNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			tx.success();
		}
		return userNode;
	}

	/**
	 * 查询POI
	 * @param poiId
	 * @return
	 */
	public static Node getPOI(String poiId) {
		Node poiNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			tx.success();
		}
		return poiNode;
	}
	
	public static ArrayList<String> getPOIs() {
		ArrayList<String> poiIds = new ArrayList<String>();
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> poiNodes = graphDb.findNodes(MyLabels.POI);
			while(poiNodes.hasNext()) {
				Node poiNode = poiNodes.next();
				poiIds.add((String)poiNode.getProperty(Consts.poiId_key));
			}
		}
		return poiIds;
	}

	/**
	 * 获取用户好友
	 * @param userId
	 * @return
	 */
	public static ArrayList<Integer> getFriends(int userId) {
		ArrayList<Integer> friends = new ArrayList<Integer>(); // 存储用户好友Id
		try (Transaction tx = graphDb.beginTx()) {
			// 根据用户Id，找出用户节点
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			System.out.printf("\nDegree of user %d is: %d\n", userId, userNode.getDegree());
			// 获取用户好友关系
			Iterable<Relationship> rs = userNode.getRelationships(RelTypes.IS_FRIEND_OF);

			for (Relationship r : rs) { // 遍历好友关系
				// 获取好友节点
				Node friendNode = r.getOtherNode(userNode);
				// 获取用户好友Id
				int friendId = (int) friendNode.getProperty(Consts.userId_key);
				// 存储用户好友Id
				friends.add(friendId);
			}

			tx.success();
		}
		return friends;
	}



	/**
	 * 查找一个用户访问过的所有兴趣点
	 * 
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIs(int userId) {
		// 兴趣点列表
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 获取目标用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) { // 用户非空
				// 获取用户的签到记录
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) {
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
	 * 
	 * @param userId
	 *            int
	 * @param startTime
	 *            String
	 * @param endTime
	 *            String
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIsByTimePeriod(int userId, String startTime, String endTime) {
		// 已访问过的兴趣点列表
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 获取用户节点
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) { // 用户节点不为空
				/*
				 * 查找到用户所有的签到记录，然后从中找出签到时间在规定范围内的签到记录
				 */
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN); // 查找用户的所有签到记录
				for (Relationship r : its) { // 根据签到记录的时间属性，比较起讫时间
					String[] timeArray = (String[]) r.getProperty(Consts.tm); // 签到时间数组
					for (String time : timeArray) { // 逐一比较
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
	 * 查询用户总签到次数
	 * 
	 * @return
	 */
	public static int getCheckinTimes(int userId) {
		int times = 0;
		try (Transaction tx = graphDb.beginTx()) {
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) {
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) {
					int weight = (int) r.getProperty(Consts.wt);
					times += weight;
				}
			}
			tx.success();
		}
		return times;
	}

	// 统计用户在某段时间内签到次数
	/**
	 * 统计用户在某段时间内的签到次数
	 * 
	 * @param userId
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static int getCheckinTimes(int userId, String startTime, String endTime) {
		int times = 0; // 签到次数
		try (Transaction tx = graphDb.beginTx()) {
			// 查找用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) { // 用户非空
				// 用户签到关系，通过签到关系的时间属性timeArray进行过滤
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) {
					// 获取签到时间列表
					String[] timeArray = (String[]) r.getProperty(Consts.tm);
					// 遍历
					for (String time : timeArray) {
						// 在时间区间内
						if ((time.compareToIgnoreCase(startTime) >= 0) && 
								(time.compareTo(endTime)) <= 0) {
							times++;
						}
					}
				}
			}
			tx.success();
		}

		return times;
	}

	/**
	 * 根据经纬度范围查找POI
	 * 
	 * @param userId
	 * @param latitude1
	 * @param latitude2
	 * @param longitude1
	 * @param longitude2
	 * @return
	 */
	public static ArrayList<String> getVisitedPOIsByArea(int userId, double latitude1, double latitude2,
			double longitude1, double longitude2) {
		// 查找区域范围内兴趣点
		ArrayList<String> visitedPOIs = new ArrayList<String>();
		// 开始
		try (Transaction tx = graphDb.beginTx()) {
			// 获取用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) {
				// 找到用户的所有签到关系
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				// 遍历
				for (Relationship r : its) {
					// 找到POI
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
	 * 
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
	 * 
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
			if (poiNode != null) { // 非空
				// 查找该POI的签到记录
				Iterable<Relationship> its = poiNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) { // 遍历
					// 签到属性：时间
					String[] timeArray = (String[]) r.getProperty(Consts.tm);
					for (String time : timeArray) {
						// 判断是否在时间区间内
						if (time.compareToIgnoreCase(startTime) > 0 && time.compareToIgnoreCase(endTime) < 0) {
							// 符合要求，记录用户
							Node userNode = r.getStartNode(); // 用户节点
							int userId = (int) userNode.getProperty(Consts.userId_key); // 用户Id
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

	/**
	 * 查询POI被访问的总次数
	 * 
	 * @param poiId
	 * @return
	 */
	public static int getVisitedTimes(String poiId) {
		int times = 0;
		try (Transaction tx = graphDb.beginTx()) {
			Node poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			if (poiNode != null) {
				Iterable<Relationship> its = poiNode.getRelationships(RelTypes.CHECKIN);
				for (Relationship r : its) {
					int weight = (int) r.getProperty(Consts.wt);
					times += weight;
				}
			}
			tx.success();
		}
		return times;
	}

	/**
	 *  统计POI被签到的热点时间：在某段时间 POI 签到次数最多。
	 *  将一天分为24个时段
	 *  
	 * @param poiId
	 * @return
	 */
	public static int[] getPOIHotTime(String poiId) {
		int[] times = new int[24];
		for (int i=0;i<24;i++) {
			times[i] = 0;
		}			
			
		try (Transaction tx = graphDb.beginTx()) {
			// 获取POI节点
			Node poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			if (poiNode != null) {	// 非空
				// get check in relationship
				Iterable<Relationship> its = poiNode.getRelationships(RelTypes.CHECKIN);
				// traverse
				for (Relationship r : its) {
					String[] timeArray = (String[]) r.getProperty(Consts.tm);
					for (String time : timeArray) {
						String hms = time.substring(11);
//						String[] dateAndHour = time.split(" ");	
						if ((hms.compareToIgnoreCase("00-") >= 0) &&
								(hms.compareToIgnoreCase("01-") < 0) ) {
							times[0]++;
						} else if ((hms.compareToIgnoreCase("01-") >= 0) &&
								(hms.compareToIgnoreCase("02-") < 0) ) {
							times[1]++;
						} else if ((hms.compareToIgnoreCase("02-") >= 0) &&
								(hms.compareToIgnoreCase("03-") < 0) ) {
							times[2]++;
						} else if ((hms.compareToIgnoreCase("03-") >= 0) &&
								(hms.compareToIgnoreCase("04-") < 0) ) {
							times[3]++;
						} else if ((hms.compareToIgnoreCase("04-") >= 0) &&
								(hms.compareToIgnoreCase("05-") < 0) ) {
							times[4]++;
						} else if ((hms.compareToIgnoreCase("05-") >= 0) &&
								(hms.compareToIgnoreCase("06-") < 0) ) {
							times[5]++;
						} else if ((hms.compareToIgnoreCase("06-") >= 0) &&
								(hms.compareToIgnoreCase("07-") < 0) ) {
							times[6]++;
						} else if ((hms.compareToIgnoreCase("07-") >= 0) &&
								(hms.compareToIgnoreCase("08-") < 0) ) {
							times[7]++;
						} else if ((hms.compareToIgnoreCase("08-") >= 0) &&
								(hms.compareToIgnoreCase("09-") < 0) ) {
							times[8]++;
						} else if ((hms.compareToIgnoreCase("09-") >= 0) &&
								(hms.compareToIgnoreCase("10-") < 0) ) {
							times[9]++;
						} else if ((hms.compareToIgnoreCase("10-") >= 0) &&
								(hms.compareToIgnoreCase("11-") < 0) ) {
							times[10]++;
						} else if ((hms.compareToIgnoreCase("11-") >= 0) &&
								(hms.compareToIgnoreCase("12-") < 0) ) {
							times[11]++;
						} else if ((hms.compareToIgnoreCase("12-") >= 0) &&
								(hms.compareToIgnoreCase("13-") < 0) ) {
							times[12]++;
						} else if ((hms.compareToIgnoreCase("13-") >= 0) &&
								(hms.compareToIgnoreCase("14-") < 0) ) {
							times[13]++;
						} else if ((hms.compareToIgnoreCase("14-") >= 0) &&
								(hms.compareToIgnoreCase("15-") < 0) ) {
							times[14]++;
						} else if ((hms.compareToIgnoreCase("15-") >= 0) &&
								(hms.compareToIgnoreCase("16-") < 0) ) {
							times[15]++;
						} else if ((hms.compareToIgnoreCase("16-") >= 0) &&
								(hms.compareToIgnoreCase("17-") < 0) ) {
							times[16]++;
						} else if ((hms.compareToIgnoreCase("17-") >= 0) &&
								(hms.compareToIgnoreCase("18-") < 0) ) {
							times[17]++;
						} else if ((hms.compareToIgnoreCase("18-") >= 0) &&
								(hms.compareToIgnoreCase("19-") < 0) ) {
							times[18]++;
						} else if ((hms.compareToIgnoreCase("19-") >= 0) &&
								(hms.compareToIgnoreCase("20-") < 0) ) {
							times[19]++;
						} else if ((hms.compareToIgnoreCase("20-") >= 0) &&
								(hms.compareToIgnoreCase("21-") < 0) ) {
							times[20]++;
						} else if ((hms.compareToIgnoreCase("21-") >= 0) &&
								(hms.compareToIgnoreCase("22-") < 0) ) {
							times[21]++;
						} else if ((hms.compareToIgnoreCase("22-") >= 0) &&
								(hms.compareToIgnoreCase("23-") < 0) ) {
							times[22]++;
						} else {
							times[23]++;
						}
					}
				}
			}
			tx.success();
		}
		return times;
	}
	
	/**
	 * 计算两个POI之间的距离
	 * 
	 * @param poiId1
	 * @param poiId2
	 * @return
	 */
	public static double getDistance(String poiId1, String poiId2) {
		double distance = 0;
		try (Transaction tx = graphDb.beginTx()) {
			Node poiNode1 = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId1);
			Node poiNode2 = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId2);
			if (poiNode1 != null && poiNode2 != null) {
				double[] loc1 = (double[]) poiNode1.getProperty(Consts.poiId_key);
				double[] loc2 = (double[]) poiNode2.getProperty(Consts.poiId_key);
				distance = getDist(loc1[0], loc1[1], loc2[0], loc2[1]);
			}
			tx.success();
		}

		return distance;
	}

	/**
	 * 计算两点之间的距离
	 * 
	 * @param latitude1
	 * @param longitude1
	 * @param latitude2
	 * @param longitude2
	 * @return
	 */
	public static double getDist(double latitude1, double longitude1, double latitude2, double longitude2) {
		double dist = 0;

		double lat1 = rad(latitude1);
		double lat2 = rad(latitude2);
		double dLat = lat1 - lat2;
		double dLon = rad(longitude1) - rad(longitude2);

		double t = Math
				.sqrt(Math.pow(Math.sin(dLat), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dLon), 2));
		dist = 2 * Math.asin(t);
		dist = dist * Consts.EARTH_RADIUS;
		dist = Math.round(dist * 10000) / 10000;

		return dist;
	}

	/**
	 * 角度转化为弧度
	 * 
	 * @param d
	 * @return
	 */
	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}

	/**
	 * 1. 获取用户所有签到位置
	 * <p>
	 * 2. 找出用户最经常签到的位置
	 * <p>
	 * 3. 找出用户签到的分布情况
	 * <p>
	 * 4. 找出用户签到时间的分布情况
	 * <p>
	 * 统计用户签到区域,
	 */
	public static void distribute(int userId) {
		int max = -1;
		Node mostPOI = null;
		double[] loc = new double[2];
		String[] timeArray = null;
		try (Transaction tx = graphDb.beginTx()) {
			// 找出用户
			Node userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			if (userNode != null) { // 非空
				// 找出用户签到关系
				Iterable<Relationship> its = userNode.getRelationships(RelTypes.CHECKIN);
				long rid = -1;
				// 遍历查找签到最多的POI
				for (Relationship r : its) {
					// 用户在某个POI上签到的次数，找出最大值
					int weight = (int) r.getProperty(Consts.wt);					
					if (weight > max) {
						rid = r.getId();
						max = weight;
					}
				}
				// 签到次数最多
				Relationship mostR = graphDb.getRelationshipById(rid);
				mostPOI = mostR.getEndNode();	// 签到次数最多的POI
				loc = (double[]) mostPOI.getProperty(Consts.loc);	// POI位置
				// 遍历查找签到最多的POI
				for (Relationship r : its) {
					// 用户在某个POI上签到的次数，找出最大值
					int weight = (int) r.getProperty(Consts.wt);					
					if (weight > max) {
						rid = r.getId();
						max = weight;
					}
				}
			}

			tx.success();
		}
	}

	/**
	 * 数据预处理
	 */
	public static void preprocess() {

	}
}
