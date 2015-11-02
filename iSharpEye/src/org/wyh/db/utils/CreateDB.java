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
	 * ������������ݽڵ��ǩlabel��
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
			//�洢poi��Ϣ��poiId���Լ�latitude,longtide
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
	 * ǩ����¼�д��ڵ��û�����Ȥ��������ݿ�
	 * 
	 * @param userId
	 * @param poiHistory
	 * @param poiLocation
	 */
	private void writeUserAndPOIToDb(int userId, HashMap<String, ArrayList<String>> poiHistory,
			HashMap<String, Double[]> poiLocation) {
		// �����û������Ƿ��Ѵ���
		Node userNode = searchUser(userId);
		if (userNode == null ){
			// �����û���������ǩ���������
			userNode = graphDb.createNode(MyLabels.USER);
			userNode.setProperty(Consts.userId_key, userId);
		}
		// ��ȡ��Ȥ����Ϣ
		Iterator<String> pois = poiHistory.keySet().iterator();
		while(pois.hasNext()) {
			// ��ȡһ����Ȥ��
			String poiId = pois.next();
			// �ҵ�����Ȥ���ǩ��ʱ��
			ArrayList<String> timeList = poiHistory.get(poiId);
			// ���뵽������
			String[] timeArray = new String[timeList.size()];
			for (int i=0; i<timeArray.length; i++) {
				timeArray[i] = timeList.get(i);
			}
			// �鿴��Ȥ��ڵ��Ƿ��Ѵ���
			Node poiNode = searchPOI(poiId);
			if (poiNode == null ){
				// ������Ȥ��ڵ�
				if(poiLocation.containsKey(poiId)) {
					System.out.println("Create POI: " + poiId);
					// ��Ȥ�㾭γ��loc
					Double[] loc = poiLocation.get(poiId);
					// ������Ȥ��ڵ㣬����ǩ������������Ϣ
					poiNode = graphDb.createNode(MyLabels.POI);
					poiNode.setProperty(Consts.poiId_key, poiId);
					poiNode.setProperty(Consts.loc, loc);
				} else {
					System.out.println("Error: create POI " + poiId);
					break;
				}
			}
			// �����û��ڵ㵽��Ȥ��ڵ��ǩ����ϵ������ʱ��������Ϊ��ϵ����
			Relationship rsh = userNode.createRelationshipTo(poiNode, RelTypes.CHECKIN);
			rsh.setProperty(Consts.tm, timeArray);
		}
		
		
	}

	/**
	 * ��friendship��ӵ����ݿ���
	 */
	private void addFriendship() {
		// 
		try (Transaction tx = graphDb.beginTx()) {
			// �洢��ȡ���ַ���
			String s = new String();
			// ���ļ���
			try (FileReader fr = new FileReader(Consts.friendshipFile)) {
				// BufferedReader��
				try (BufferedReader br = new BufferedReader(fr)) {
					// ÿ�б�ʾһ�����ѹ�ϵ���������û�����"\t"�ָ�
					String info[];
					// ��ȡһ��
					while ( (s = br.readLine()) != null) {
						// �ַ����ָ�
						info = s.split("\t");
						
						// ��ȡ�û��ڵ㣬���������򲻴������ѹ�ϵ
						Node firstNode = searchUser(Integer.parseInt(info[0].trim()));
						Node secondNode = searchUser(Integer.parseInt(info[1].trim()));
						// �����û�����ǩ����¼
						if (firstNode != null && secondNode != null) {
							// isFriend��ʶ�����Ƿ�Ϊ���ѹ�ϵ
							boolean isFriend = false;
							// ��ȡ�û�1�����к��ѹ�ϵ
							Iterable<Relationship> itr = firstNode.getRelationships(RelTypes.IS_FRIEND_OF);
							for (Relationship rs : itr) {
								// �����鿴�û�1���û�2�Ƿ�Ϊ����
								if (rs.getOtherNode(firstNode) == secondNode) {
									isFriend = true; // �Ǻ���
								}
							}
							// ����δ���ں��ѹ�ϵ
							if (!isFriend) {
								// �������ѹ�ϵ
								firstNode.createRelationshipTo(secondNode, RelTypes.IS_FRIEND_OF);
							}
						} else if (firstNode == null) {
							// �û�1�ڵ㲻����
							System.out.println("first user " + info[0].trim() + " does not exist in checkins.");
						} else if (secondNode == null ){
							// �û�2�ڵ㲻����
							System.out.println("second user " + info[1].trim() + " does not exist in checkins.");
						}
					}
					// �ر�bufferedreader��
					br.close();
				}
				// �ر��ļ���
				fr.close();
			} catch (FileNotFoundException e) {
				// �ļ��Ҳ����쳣
				e.printStackTrace();
			} catch (IOException e) {
				// IO�쳣
				e.printStackTrace();
			}
			tx.success();
		}
		
	}
	
	/**
	 * �����û�id�����û�
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
	 * ������Ȥ��id����POI
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
