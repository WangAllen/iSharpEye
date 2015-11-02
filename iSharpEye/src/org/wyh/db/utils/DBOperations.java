package org.wyh.db.utils;

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
}
