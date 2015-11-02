package org.wyh.db.utils;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class DatabaseUtils {
	/**
	 * ȷ��ͼ���ݿ��ܹ��رգ���ʹʹ��ctrl-c ǿ���˳�VM��
	 * @param graphDb
	 */
	public static void registerShutdownHook(GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
	
	/**
	 * ��path�£���ȡGraphDatabaseService
	 * @param path
	 * @return
	 */
	public static GraphDatabaseService getDbService(String path) {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(path));
		registerShutdownHook(graphDb);
		return graphDb;
	}
	
	
	/**
	 * ɾ��ȫ�����ݿ��ļ�
	 * @param path
	 * @return
	 */
	public static boolean clearDb(String path) {
		boolean cFlag = false;
		try {
			FileUtils.deleteRecursively(new File(path));
			cFlag = true;
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		return cFlag;
	}
	
	public static void shutdownDb(GraphDatabaseService graphDb) {
		graphDb.shutdown();
	}
}
