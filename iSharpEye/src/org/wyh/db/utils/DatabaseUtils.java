package org.wyh.db.utils;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class DatabaseUtils {
	/**
	 * 确保图数据库能够关闭（即使使用ctrl-c 强制退出VM）
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
	 * 在path下，获取GraphDatabaseService
	 * @param path
	 * @return
	 */
	public static GraphDatabaseService getDbService(String path) {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(path));
		registerShutdownHook(graphDb);
		return graphDb;
	}
	
	
	/**
	 * 删除全部数据库文件
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
