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
				try (BufferedReader input = new BufferedReader(fr)) {
					while ((s = input.readLine()) !=null ) {
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
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}

	private void writeUserAndPOIToDb(int userId, HashMap<String, ArrayList<String>> poiHistory,
			HashMap<String, Double[]> poiLocation) {


		Node userNode = searchUser(userId);
		if (userNode == null ){
			userNode = graphDb.createNode(MyLabels.USER);
			userNode.setProperty(Consts.userId_key, userId);
		}
		
		Iterator<String> pois = poiHistory.keySet().iterator();
		while(pois.hasNext()) {
			String poiId = pois.next();
			ArrayList<String> timeList = poiHistory.get(poiId);
			String[] timeArray = new String[timeList.size()];
			for (int i=0; i<timeArray.length; i++) {
				timeArray[i] = timeList.get(i);
			}
			
			Node poiNode = searchPOI(poiId);
			if (poiNode == null ){
				if(poiLocation.containsKey(poiId)) {
					System.out.println("Create POI: " + poiId);
					Double[] loc = poiLocation.get(poiId);
					poiNode = graphDb.createNode(MyLabels.POI);
					poiNode.setProperty(Consts.poiId_key, poiId);
					poiNode.setProperty(Consts.loc, loc);
				} else {
					System.out.println("Error: create POI " + poiId);
					break;
				}
			}
			
			Relationship rsh = userNode.createRelationshipTo(poiNode, RelTypes.CHECKIN);
			rsh.setProperty(Consts.tm, timeArray);
		}
		
		
	}

	private Node searchUser(int userId) {
		Node userNode = null;
		try(Transaction tx = graphDb.beginTx() ) {
			userNode = graphDb.findNode(MyLabels.USER, Consts.userId_key, userId);
			tx.success();
		}
		return userNode;
	}
	
	private Node searchPOI(String poiId) {
		Node poiNode = null;
		try(Transaction tx = graphDb.beginTx() ) {
			poiNode = graphDb.findNode(MyLabels.POI, Consts.poiId_key, poiId);
			tx.success();
		}
		return poiNode;
	}
}
