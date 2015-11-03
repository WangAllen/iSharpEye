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
 * ����ͼ���ݿ�
 * 
 * @author wail
 *
 */
public class CreateDB {

	private final GraphDatabaseService graphDb;

	public CreateDB(String path) {
		// ��ȡͼ���ݿ����
		graphDb = DatabaseUtils.getDbService(path);
		// �������ݿ�
		// reset();
		// �������
		// addIndex();
		// ����ͼ���ݿ�
		createGraph(path);
	}

	/**
	 * ������������ݽڵ��ǩlabel��
	 */
	public void addIndex() {
		// ��ʼ����
		try (Transaction tx = graphDb.beginTx()) {
			// ��ȡSchema
			Schema schema = graphDb.schema();
			// ΪUser�������
			schema.indexFor(MyLabels.USER).on(Consts.userId_key).create();
			// ΪPOI�������
			schema.indexFor(MyLabels.POI).on(Consts.poiId_key).create();
			// �ύ
			tx.success();
		}
	}

	/**
	 * �������ݿ�
	 */
	public void reset() {
		try (Transaction tx = graphDb.beginTx()) {
			// ɾ������
			Iterable<IndexDefinition> indexDef = graphDb.schema().getIndexes();
			for (IndexDefinition i : indexDef) {
				i.drop();
			}

			GlobalGraphOperations ggo = GlobalGraphOperations.at(graphDb);
			// ɾ����ϵ
			for (Relationship r : ggo.getAllRelationships()) {
				r.delete();
			}
			// ɾ���ڵ�
			for (Node n : ggo.getAllNodes()) {
				n.delete();
			}
			tx.success();
		}
	}

	/**
	 * �������ݿ�
	 * 
	 * @param path
	 */
	public void createGraph(String path) {
		// createCheckinGraph();
		// addFriendship();
		dataProcess(dataPreprocess());
	}

	/**
	 * ����ǩ��ͼ
	 */
	private void createCheckinGraph() {
		// try (Transaction tx = graphDb.beginTx()) {
		String s = new String(); // �洢�����ļ��е�һ��
		String substrings[]; // ���ڴ洢�ַ�����ֵ��Ӵ�
		String lastUser = ""; // ǰһ�û�

		/*
		 * һ���û���ͬһ��POI����ǩ����Σ���ˣ���������ͬһPOIǩ����ʱ����õ�һ��ʱ�������У�����Ϊ ��ǩ������ϵ��������Ϣ��
		 */
		HashMap<String, ArrayList<String>> poiHistory = new HashMap<String, ArrayList<String>>();
		// �洢poi��Ϣ��poiId���Լ�latitude,longtide
		HashMap<String, Double[]> poiLocation = new HashMap<String, Double[]>();

		// ��ȡǩ�������ļ�
		try (FileReader fr = new FileReader(Consts.checkinFile)) {
			// read stream
			try (BufferedReader br = new BufferedReader(fr)) {
				while ((s = br.readLine()) != null) { // ��ȡһ�У����ж��Ƿ����һ��
					substrings = s.split("\t"); // ����ַ���
					if (substrings.length < 5) { // �ж��ַ����Ƿ�Ϸ�
						if (substrings.length == 0) {
							System.out.println("Something is wrong with XXXXXXXXXXXXXXXXXXXXXX");
						} else {
							System.out.println("Something is wrong with " + substrings[0].trim());
						}
						break;
					}
					/*
					 * ǩ���ĸ�ʽ�� userId, Checkin_time, latitude, longitude, poiId
					 */
					String currentUser = substrings[0].trim();
					String currentPOI = substrings[4].trim();
					String currentTime = substrings[1].replace('T', ' ').replace('Z', ' ').trim();
					// ��ЧPOI
					if (currentPOI.equalsIgnoreCase("00000000000000000000000000000000")) {
						continue;
					}
					// poiLocation��¼���е�POI����
					if (!poiLocation.containsKey(currentPOI)) {
						Double[] loc = { Double.parseDouble(substrings[2].trim()),
								Double.parseDouble(substrings[3].trim()) };

						poiLocation.put(currentPOI, loc);
					}

					// �ж��Ƿ�ǰ�û���ǰһ��ǩ�����û�Ϊͬһ��
					if (currentUser.equalsIgnoreCase(lastUser)) {
						// �жϵ�ǰ�û���ǩ����ʷ���Ƿ��Ѿ����ڵ�ǰ��POI
						if (poiHistory.containsKey(currentPOI)) { // �ڵ�ǰλ�����й�ǩ��
							ArrayList<String> timeList = new ArrayList<String>();
							timeList = poiHistory.get(currentPOI);
							timeList.add(currentTime);
							poiHistory.put(currentPOI, timeList);
							lastUser = currentUser;
							continue;
						} else { // ��ǰλ�û�δ�й�ǩ��
							ArrayList<String> timeList = new ArrayList<String>();
							timeList.add(currentTime);
							poiHistory.put(currentPOI, timeList);
							lastUser = currentUser;
							continue;
						}
					} else { // ��ǰ�û�����ǰһ�û�
						if (!lastUser.isEmpty()) { // ��ǰһ�û���Ϊ�գ����ڿ�ʼ�������ݿ�ʱ��
							// ��ǰһ�û���ǩ����Ϣ���������ݿ���
							writeUserAndPOIToDb(Integer.parseInt(lastUser), poiHistory, poiLocation);
							// ǰһ�û���ǩ����ʷ���
							poiHistory.clear();
						}
						// ����ǰ��ǩ����Ϣ��¼
						ArrayList<String> timeList = new ArrayList<String>();
						timeList.add(currentTime);
						poiHistory.put(currentPOI, timeList);
						lastUser = currentUser;
					}
				}
				// ���һ���û���ǩ����Ϣд�뵽���ݿ�
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
	 * ǩ����¼�д��ڵ��û�����Ȥ��������ݿ�
	 * 
	 * @param userId
	 * @param poiHistory
	 * @param poiLocation
	 */
	private void writeUserAndPOIToDb(int userId, HashMap<String, ArrayList<String>> poiHistory,
			HashMap<String, Double[]> poiLocation) {
		try (Transaction tx = graphDb.beginTx()) {
			System.out.println("Writing user and poi into graph~~~");
			// �����û������Ƿ��Ѵ���
			Node userNode = searchUser(userId);
			if (userNode == null) {
				// �����û���������ǩ���������
				userNode = graphDb.createNode(MyLabels.USER);
				userNode.setProperty(Consts.userId_key, userId);
			}
			// ��ȡ��Ȥ����Ϣ
			Iterator<String> pois = poiHistory.keySet().iterator();
			while (pois.hasNext()) {
				// ��ȡһ����Ȥ��
				String poiId = pois.next();
				// �ҵ�����Ȥ���ǩ��ʱ��
				ArrayList<String> timeList = poiHistory.get(poiId);
				// ���뵽������
				String[] timeArray = new String[timeList.size()];
				for (int i = 0; i < timeArray.length; i++) {
					timeArray[i] = timeList.get(i);
				}
				// �鿴��Ȥ��ڵ��Ƿ��Ѵ���
				Node poiNode = searchPOI(poiId);
				if (poiNode == null) {
					// ������Ȥ��ڵ�
					if (poiLocation.containsKey(poiId)) {
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
				// ��ǩ��������Ϊ��ϵȨ��
				rsh.setProperty(Consts.wt, timeArray.length);
			}
			tx.success();
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
					while ((s = br.readLine()) != null) {
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
								System.out.println("Created Friendship between " + info[0].trim() + " and "
										+ info[1].trim() + ".");
							}
						} else if (firstNode == null) {
							// �û�1�ڵ㲻����
							System.out.println("first user " + info[0].trim() + " does not exist in checkins.");
						} else if (secondNode == null) {
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
	 * ������Ȥ��id����POI
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
	 * Ԥ���������в�����Ҫ����û���POI�����ݿ�ɾ���� ���û�������С��10�ˣ����û�ǩ������С��10�Σ�������Ҫ��
	 */
	public ArrayList<Long> dataPreprocess() {
		// ��Ҫɾ�������û��ڵ�Id
		ArrayList<Long> nodeToRemove = new ArrayList<Long>();
		try (Transaction tx = graphDb.beginTx()) {
			// ��ȡ�����û�
			ResourceIterator<Node> users = graphDb.findNodes(MyLabels.USER);
			// �����û�
			while (users.hasNext()) {
				Node userNode = users.next();
				long userNodeId = userNode.getId(); // �û��ڵ�Id
				int friendDegree = userNode.getDegree(RelTypes.IS_FRIEND_OF); // ������
				// ������ĿС��10������ɾ���б�
				if (friendDegree < 10) {
					System.out.printf("user %d's friends number is %d", (int) userNode.getProperty(Consts.userId_key),
							friendDegree);
					// �鿴ɾ���б��Ƿ��Ѿ�������
					if (!nodeToRemove.contains(userNodeId))
						nodeToRemove.add(userNodeId);
				}
				// �鿴�û�ǩ�������Ƿ����Ҫ����ǩ������С��10�Σ������ϣ�
				Iterable<Relationship> rs = userNode.getRelationships(RelTypes.CHECKIN); // ����ǩ����ϵ
				int checkinTimes = 0; // ��ǩ������
				for (Relationship r : rs) {
					// ͨ��Ȩ��weight����������ǩ������
					checkinTimes += (int) r.getProperty(Consts.wt);
				}
				if (checkinTimes < 10) { // ��ǩ������С��10�Σ�����ɾ���б�
					System.out.println("\t\n and checkin times is " + checkinTimes);
					if (!nodeToRemove.contains(userNodeId))
						nodeToRemove.add(userNodeId);
				}
			}

			// ��ȡ����POI
			ResourceIterator<Node> pois = graphDb.findNodes(MyLabels.POI);
			while (pois.hasNext()) {
				Node poiNode = pois.next();
				long nodeId = poiNode.getId(); // poiNodeId
				Iterable<Relationship> rs = poiNode.getRelationships(RelTypes.CHECKIN);// ����ǩ����ϵ
				int checkinTimes = 0;
				for (Relationship r : rs) {
					// ���㱻ǩ������
					checkinTimes += (int) r.getProperty(Consts.wt);
				}

				if (checkinTimes < 10) { // ��ǩ���ܴ���С��10��
					System.out.printf("POI %s checkedin times is %d\t\n.", (String) poiNode.getProperty(Consts.poiId_key),
							checkinTimes);
					if (!nodeToRemove.contains(nodeId))
						nodeToRemove.add(nodeId);
				}
			}

//			// remove data����ɾ���б�ɾ������
//			for (long id : nodeToRemove) {
//				// ͨ��id�ҳ���Ҫɾ���Ľڵ�
//				Node node = graphDb.getNodeById(id);
//				System.out.println("ɾ���ڵ�" + id);
//				// �ҳ����д�ɾ���ڵ�Ĺ�����ϵ
//				Iterable<Relationship> itr = node.getRelationships();
//				for (Relationship r : itr) {
//					// ɾ�����й�ϵ
//					r.delete();
//				}
//				// ɾ���ڵ�
//				node.delete();
//			}

			tx.success(); // �ύ
		}

		return nodeToRemove;
	}

	public void dataProcess(ArrayList<Long> nodeToRemove) {
		// remove data����ɾ���б�ɾ������
		for (long id : nodeToRemove) {
			try (Transaction tx = graphDb.beginTx()) {
				// ͨ��id�ҳ���Ҫɾ���Ľڵ�
				Node node = graphDb.getNodeById(id);
				System.out.println("ɾ���ڵ�" + id);
				// �ҳ����д�ɾ���ڵ�Ĺ�����ϵ
				Iterable<Relationship> itr = node.getRelationships();
				for (Relationship r : itr) {
					// ɾ�����й�ϵ
					r.delete();
				}
				// ɾ���ڵ�
				node.delete();
				
				tx.success();
			}
		}
	}
	// End of Class CreateDB
}
