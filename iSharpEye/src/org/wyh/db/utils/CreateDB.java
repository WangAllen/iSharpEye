package org.wyh.db.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * 创建图数据库
 * 
 * @author wail
 *
 */
public class CreateDB {

	private final GraphDatabaseService graphDb;

	public CreateDB(String path) {
		// 获取图数据库服务
		graphDb = DatabaseUtils.getDbService(path);
		// 重置数据库
		// reset();
		// 添加索引
		// addIndex();
		// 创建图数据库
		createGraph(path);
	}

	/**
	 * 添加索引（根据节点标签label）
	 */
	public void addIndex() {
		// 开始事务
		try (Transaction tx = graphDb.beginTx()) {
			// 获取Schema
			Schema schema = graphDb.schema();
			// 为User添加索引
			schema.indexFor(MyLabels.USER).on(Consts.userId_key).create();
			// 为POI添加索引
			schema.indexFor(MyLabels.POI).on(Consts.poiId_key).create();
			// 提交
			tx.success();
		}
	}

	/**
	 * 重置数据库
	 */
	public void reset() {
		try (Transaction tx = graphDb.beginTx()) {
			// 删除索引
			Iterable<IndexDefinition> indexDef = graphDb.schema().getIndexes();
			for (IndexDefinition i : indexDef) {
				i.drop();
			}

			GlobalGraphOperations ggo = GlobalGraphOperations.at(graphDb);
			// 删除关系
			for (Relationship r : ggo.getAllRelationships()) {
				r.delete();
			}
			// 删除节点
			for (Node n : ggo.getAllNodes()) {
				n.delete();
			}
			tx.success();
		}
	}

	/**
	 * 创建数据库
	 * 
	 * @param path
	 */
	public void createGraph(String path) {
		// createCheckinGraph();
		// addFriendship();
		dataProcess(dataPreprocess());
	}

	/**
	 * 创建签到图
	 */
	private void createCheckinGraph() {
		// try (Transaction tx = graphDb.beginTx()) {
		String s = new String(); // 存储数据文件中的一行
		String substrings[]; // 用于存储字符串拆分的子串
		String lastUser = ""; // 前一用户

		/*
		 * 一个用户在同一个POI可能签到多次，因此，将所有在同一POI签到的时间放置到一个时间数组中，并作为 “签到”关系的属性信息。
		 */
		HashMap<String, ArrayList<String>> poiHistory = new HashMap<String, ArrayList<String>>();
		// 存储poi信息：poiId，以及latitude,longtide
		HashMap<String, Double[]> poiLocation = new HashMap<String, Double[]>();

		// 读取签到数据文件
		try (FileReader fr = new FileReader(Consts.checkinFile)) {
			// read stream
			try (BufferedReader br = new BufferedReader(fr)) {
				while ((s = br.readLine()) != null) { // 获取一行，并判断是否最后一行
					substrings = s.split("\t"); // 拆分字符串
					if (substrings.length < 5) { // 判断字符串是否合法
						if (substrings.length == 0) {
							System.out.println("Something is wrong with XXXXXXXXXXXXXXXXXXXXXX");
						} else {
							System.out.println("Something is wrong with " + substrings[0].trim());
						}
						break;
					}
					/*
					 * 签到的格式： userId, Checkin_time, latitude, longitude, poiId
					 */
					String currentUser = substrings[0].trim();
					String currentPOI = substrings[4].trim();
					String currentTime = substrings[1].replace('T', ' ').replace('Z', ' ').trim();
					// 无效POI
					if (currentPOI.equalsIgnoreCase("00000000000000000000000000000000")) {
						continue;
					}
					// poiLocation记录所有的POI数据
					if (!poiLocation.containsKey(currentPOI)) {
						Double[] loc = { Double.parseDouble(substrings[2].trim()),
								Double.parseDouble(substrings[3].trim()) };

						poiLocation.put(currentPOI, loc);
					}

					// 判断是否当前用户跟前一个签到的用户为同一个
					if (currentUser.equalsIgnoreCase(lastUser)) {
						// 判断当前用户的签到历史中是否已经存在当前的POI
						if (poiHistory.containsKey(currentPOI)) { // 在当前位置已有过签到
							ArrayList<String> timeList = new ArrayList<String>();
							timeList = poiHistory.get(currentPOI);
							timeList.add(currentTime);
							poiHistory.put(currentPOI, timeList);
							lastUser = currentUser;
							continue;
						} else { // 当前位置还未有过签到
							ArrayList<String> timeList = new ArrayList<String>();
							timeList.add(currentTime);
							poiHistory.put(currentPOI, timeList);
							lastUser = currentUser;
							continue;
						}
					} else { // 当前用户不是前一用户
						if (!lastUser.isEmpty()) { // 若前一用户不为空（用于开始创建数据库时）
							// 将前一用户的签到信息、存入数据库中
							writeUserAndPOIToDb(Integer.parseInt(lastUser), poiHistory, poiLocation);
							// 前一用户的签到历史清空
							poiHistory.clear();
						}
						// 将当前的签到信息记录
						ArrayList<String> timeList = new ArrayList<String>();
						timeList.add(currentTime);
						poiHistory.put(currentPOI, timeList);
						lastUser = currentUser;
					}
				}
				// 最后一个用户的签到信息写入到数据库
				if (!lastUser.isEmpty()) {
					writeUserAndPOIToDb(Integer.parseInt(lastUser), poiHistory, poiLocation);
					System.out.println("Last user's Checkin ================");
				}
				br.close();
			}
			fr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// tx.success();
		// }
	}

	/**
	 * 签到记录中存在的用户和兴趣点存入数据库
	 * 
	 * @param userId
	 * @param poiHistory
	 * @param poiLocation
	 */
	private void writeUserAndPOIToDb(int userId, HashMap<String, ArrayList<String>> poiHistory,
			HashMap<String, Double[]> poiLocation) {
		try (Transaction tx = graphDb.beginTx()) {
			System.out.println("Writing user and poi into graph~~~");
			// 查找用户，看是否已存在
			Node userNode = searchUser(userId);
			if (userNode == null) {
				// 创建用户，并做标签，添加属性
				userNode = graphDb.createNode(MyLabels.USER);
				userNode.setProperty(Consts.userId_key, userId);
			}
			// 获取兴趣点信息
			Iterator<String> pois = poiHistory.keySet().iterator();
			while (pois.hasNext()) {
				// 获取一个兴趣点
				String poiId = pois.next();
				// 找到该兴趣点的签到时间
				ArrayList<String> timeList = poiHistory.get(poiId);
				// 存入到数组中
				String[] timeArray = new String[timeList.size()];
				for (int i = 0; i < timeArray.length; i++) {
					timeArray[i] = timeList.get(i);
				}
				// 查看兴趣点节点是否已存在
				Node poiNode = searchPOI(poiId);
				if (poiNode == null) {
					// 创建兴趣点节点
					if (poiLocation.containsKey(poiId)) {
						System.out.println("Create POI: " + poiId);
						// 兴趣点经纬度loc
						Double[] loc = poiLocation.get(poiId);
						// 创建兴趣点节点，贴标签，设置属性信息
						poiNode = graphDb.createNode(MyLabels.POI);
						poiNode.setProperty(Consts.poiId_key, poiId);
						poiNode.setProperty(Consts.loc, loc);
					} else {
						System.out.println("Error: create POI " + poiId);
						break;
					}
				}
				// 创建用户节点到兴趣点节点的签到关系，并将时间数组作为关系属性
				Relationship rsh = userNode.createRelationshipTo(poiNode, RelTypes.CHECKIN);
				rsh.setProperty(Consts.tm, timeArray);
				// 以签到次数作为关系权重
				rsh.setProperty(Consts.wt, timeArray.length);
			}
			tx.success();
		}
	}

	/**
	 * 将friendship添加到数据库中
	 */
	private void addFriendship() {
		//
		try (Transaction tx = graphDb.beginTx()) {
			// 存储读取的字符串
			String s = new String();
			// 打开文件流
			try (FileReader fr = new FileReader(Consts.friendshipFile)) {
				// BufferedReader流
				try (BufferedReader br = new BufferedReader(fr)) {
					// 每行表示一个好友关系，有两个用户，用"\t"分割
					String info[];
					// 读取一行
					while ((s = br.readLine()) != null) {
						// 字符串分割
						info = s.split("\t");

						// 获取用户节点，若不存在则不创建好友关系
						Node firstNode = searchUser(Integer.parseInt(info[0].trim()));
						Node secondNode = searchUser(Integer.parseInt(info[1].trim()));
						// 两个用户均有签到记录
						if (firstNode != null && secondNode != null) {
							// isFriend标识二者是否为好友关系
							boolean isFriend = false;
							// 获取用户1的所有好友关系
							Iterable<Relationship> itr = firstNode.getRelationships(RelTypes.IS_FRIEND_OF);
							for (Relationship rs : itr) {
								// 遍历查看用户1与用户2是否为好友
								if (rs.getOtherNode(firstNode) == secondNode) {
									isFriend = true; // 是好友
								}
							}
							// 若还未存在好友关系
							if (!isFriend) {
								// 创建好友关系
								firstNode.createRelationshipTo(secondNode, RelTypes.IS_FRIEND_OF);
								System.out.println("Created Friendship between " + info[0].trim() + " and "
										+ info[1].trim() + ".");
							}
						} else if (firstNode == null) {
							// 用户1节点不存在
							System.out.println("first user " + info[0].trim() + " does not exist in checkins.");
						} else if (secondNode == null) {
							// 用户2节点不存在
							System.out.println("second user " + info[1].trim() + " does not exist in checkins.");
						}
					}
					// 关闭bufferedreader流
					br.close();
				}
				// 关闭文件流
				fr.close();
			} catch (FileNotFoundException e) {
				// 文件找不到异常
				e.printStackTrace();
			} catch (IOException e) {
				// IO异常
				e.printStackTrace();
			}
			tx.success();
		}

	}

	/**
	 * 根据用户id检索用户
	 * 
	 * @param userId
	 * @return
	 */
	private Node searchUser(int userId) {
		Node userNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			tx.success();
		}
		return userNode;
	}

	/**
	 * 根据兴趣点id搜索POI
	 * 
	 * @param poiId
	 * @return
	 */
	private Node searchPOI(String poiId) {
		Node poiNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			tx.success();
		}
		return poiNode;
	}

	/**
	 * 预处理，将所有不符合要求的用户和POI从数据库删除。 若用户好友数小于10人，或用户签到次数小于10次，不符合要求
	 */
	public ArrayList<Long> dataPreprocess() {
		// 需要删除掉的用户节点Id
		ArrayList<Long> nodeToRemove = new ArrayList<Long>();
		try (Transaction tx = graphDb.beginTx()) {
			// 获取所有用户
			ResourceIterator<Node> users = graphDb.findNodes(MyLabels.USER);
			// 遍历用户
			while (users.hasNext()) {
				Node userNode = users.next();
				long userNodeId = userNode.getId(); // 用户节点Id
				int friendDegree = userNode.getDegree(RelTypes.IS_FRIEND_OF); // 好友数
				// 好友数目小于10，存入删除列表
				if (friendDegree < 10) {
					System.out.printf("user %d's friends number is %d", (int) userNode.getProperty(Consts.userId_key),
							friendDegree);
					// 查看删除列表是否已经包含了
					if (!nodeToRemove.contains(userNodeId))
						nodeToRemove.add(userNodeId);
				}
				// 查看用户签到次数是否符合要求（总签到次数小于10次，不符合）
				Iterable<Relationship> rs = userNode.getRelationships(RelTypes.CHECKIN); // 所有签到关系
				int checkinTimes = 0; // 总签到次数
				for (Relationship r : rs) {
					// 通过权重weight，计算所有签到次数
					checkinTimes += (int) r.getProperty(Consts.wt);
				}
				if (checkinTimes < 10) { // 总签到次数小于10次，放入删除列表
					System.out.println("\t\n and checkin times is " + checkinTimes);
					if (!nodeToRemove.contains(userNodeId))
						nodeToRemove.add(userNodeId);
				}
			}

			// 获取所有POI
			ResourceIterator<Node> pois = graphDb.findNodes(MyLabels.POI);
			while (pois.hasNext()) {
				Node poiNode = pois.next();
				long nodeId = poiNode.getId(); // poiNodeId
				Iterable<Relationship> rs = poiNode.getRelationships(RelTypes.CHECKIN);// 所有签到关系
				int checkinTimes = 0;
				for (Relationship r : rs) {
					// 计算被签到次数
					checkinTimes += (int) r.getProperty(Consts.wt);
				}

				if (checkinTimes < 10) { // 被签到总次数小于10次
					System.out.printf("POI %s checkedin times is %d\t\n.", (String) poiNode.getProperty(Consts.poiId_key),
							checkinTimes);
					if (!nodeToRemove.contains(nodeId))
						nodeToRemove.add(nodeId);
				}
			}

//			// remove data根据删除列表，删除数据
//			for (long id : nodeToRemove) {
//				// 通过id找出需要删除的节点
//				Node node = graphDb.getNodeById(id);
//				System.out.println("删除节点" + id);
//				// 找出所有待删除节点的关联关系
//				Iterable<Relationship> itr = node.getRelationships();
//				for (Relationship r : itr) {
//					// 删除所有关系
//					r.delete();
//				}
//				// 删除节点
//				node.delete();
//			}

			tx.success(); // 提交
		}

		return nodeToRemove;
	}

	public void dataProcess(ArrayList<Long> nodeToRemove) {
		// remove data根据删除列表，删除数据
		for (long id : nodeToRemove) {
			try (Transaction tx = graphDb.beginTx()) {
				// 通过id找出需要删除的节点
				Node node = graphDb.getNodeById(id);
				System.out.println("删除节点" + id);
				// 找出所有待删除节点的关联关系
				Iterable<Relationship> itr = node.getRelationships();
				for (Relationship r : itr) {
					// 删除所有关系
					r.delete();
				}
				// 删除节点
				node.delete();
				
				tx.success();
			}
		}
	}
	// End of Class CreateDB
}
