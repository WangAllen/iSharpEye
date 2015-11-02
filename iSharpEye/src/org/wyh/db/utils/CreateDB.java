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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

public class CreateDB {

	private final GraphDatabaseService graphDb;
	
	public CreateDB(String path) {
		graphDb = DatabaseUtils.getDbService(path);
		reset();
		addIndex();
		createGraph(path);
	}
	
	/**
	 * 添加索引（根据节点标签label）
	 */
	public void addIndex(){
		try (Transaction tx = graphDb.beginTx()	) {
			Schema schema = graphDb.schema();
			schema.indexFor(MyLabels.USER).on(Consts.userId_key).create();
			schema.indexFor(MyLabels.POI).on(Consts.poiId_key).create();
			tx.success();
		}
	}
	
	public void reset() {
		try (Transaction tx = graphDb.beginTx()) {
			Iterable<IndexDefinition> indexDef = graphDb.schema().getIndexes();
			for (IndexDefinition i : indexDef) {
				i.drop();
			}
			GlobalGraphOperations ggo = GlobalGraphOperations.at(graphDb);
			for (Relationship r : ggo.getAllRelationships()) {
				r.delete();
			}
			for (Node n : ggo.getAllNodes()) {
				n.delete();
			}
			tx.success();
		}
	}
	
	public void createGraph(String path) {
		createCheckinGraph();
		addFriendship();
	}


	private void createCheckinGraph() {
		try (Transaction tx = graphDb.beginTx()) {
			String s = new String();
			String substrings[];
			String lastUser = "";
			
			HashMap<String, ArrayList<String>> poiHistory = new HashMap<String, ArrayList<String>>();
			//存储poi信息：poiId，以及latitude,longtide
			HashMap<String, Double[]> poiLocation = new HashMap<String, Double[]>();
			
			try (FileReader fr = new FileReader(Consts.checkinFile)) {
				try (BufferedReader br = new BufferedReader(fr)) {
					while ((s = br.readLine()) !=null ) {
						substrings = s.split("\t");
						if (substrings.length < 5) {
							if(substrings.length == 0){
								System.out.println("Something is wrong with XXXXXXXXXXXXXXXXXXXXXX");
							} else {
								
								System.out.println("Something is wrong with " + substrings[0].trim());
							}
							break;
						}
						
						String currentUser = substrings[0].trim();
						String currentPOI = substrings[4].trim();
						String currentTime = substrings[1].replace('T', ' ').replace('Z', ' ').trim();
						if (currentPOI.equalsIgnoreCase("00000000000000000000000000000000")) {
							continue;
						}
						if(!poiLocation.containsKey(currentPOI)) {
							Double[] loc = {
									Double.parseDouble(substrings[2].trim()),
									Double.parseDouble(substrings[3].trim())
							};
							
							poiLocation.put(currentPOI, loc);
						}
						
						if (currentUser.equalsIgnoreCase(lastUser)) {
							if (poiHistory.containsKey(currentPOI)) {
								ArrayList<String> timeList = new ArrayList<String>();
								timeList = poiHistory.get(currentPOI);
								timeList.add(currentTime);
								poiHistory.put(currentPOI, timeList);
								lastUser = currentUser;
								continue;
							} else {
								ArrayList<String> timeList = new ArrayList<String>();
								timeList.add(currentTime);
								poiHistory.put(currentPOI, timeList);
								lastUser = currentUser;
								continue;
							}
						} else {
							if (!lastUser.isEmpty()) {
								writeUserAndPOIToDb(Integer.parseInt(lastUser), poiHistory, poiLocation);
								poiHistory.clear();
							}
							ArrayList<String> timeList = new ArrayList<String>();
							timeList.add(currentTime);
							poiHistory.put(currentPOI, timeList);
							lastUser = currentUser;
						}
						
						if(!lastUser.isEmpty()) {
							writeUserAndPOIToDb(Integer.parseInt(lastUser), poiHistory, poiLocation);
						}
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
			
		}
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
		// 查找用户，看是否已存在
		Node userNode = searchUser(userId);
		if (userNode == null ){
			// 创建用户，并做标签，添加属性
			userNode = graphDb.createNode(MyLabels.USER);
			userNode.setProperty(Consts.userId_key, userId);
		}
		// 获取兴趣点信息
		Iterator<String> pois = poiHistory.keySet().iterator();
		while(pois.hasNext()) {
			// 获取一个兴趣点
			String poiId = pois.next();
			// 找到该兴趣点的签到时间
			ArrayList<String> timeList = poiHistory.get(poiId);
			// 存入到数组中
			String[] timeArray = new String[timeList.size()];
			for (int i=0; i<timeArray.length; i++) {
				timeArray[i] = timeList.get(i);
			}
			// 查看兴趣点节点是否已存在
			Node poiNode = searchPOI(poiId);
			if (poiNode == null ){
				// 创建兴趣点节点
				if(poiLocation.containsKey(poiId)) {
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
					while ( (s = br.readLine()) != null) {
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
							}
						} else if (firstNode == null) {
							// 用户1节点不存在
							System.out.println("first user " + info[0].trim() + " does not exist in checkins.");
						} else if (secondNode == null ){
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
	 * @param userId
	 * @return
	 */
	private Node searchUser(int userId) {
		Node userNode = null;
		try(Transaction tx = graphDb.beginTx() ) {
			userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			tx.success();
		}
		return userNode;
	}
	
	/**
	 * 根据兴趣点id搜索POI
	 * @param poiId
	 * @return
	 */
	private Node searchPOI(String poiId) {
		Node poiNode = null;
		try(Transaction tx = graphDb.beginTx() ) {
			poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			tx.success();
		}
		return poiNode;
	}
}
