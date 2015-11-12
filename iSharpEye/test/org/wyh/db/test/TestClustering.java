package org.wyh.db.test;

import org.wyh.db.utils.Clustering;

public class TestClustering {

	public static void test_generatePoints() {
		int number = 10;
		double[][] points = Clustering.generatePoints(number);
		for (int i=0;i<points.length;i++) {
			System.out.println("" + points[i][0] + "\t" + points[i][1]);
		}
	}
	
	public static void test_doCluster() {
		int k = 10;
		int pointsNumber = 10;
		double[][] points = Clustering.generatePoints(pointsNumber);
		double[][][] cluster = Clustering.doCluster(points, k);
		for (int i=0;i<1;i++) {
			for (int j=0;j<cluster[i].length;j++) {
				System.out.println("" + cluster[i][j][0] + "\t" + cluster[i][j][1]);
			}
		}
	}
	
	public static void main(String[] args) {
//		test_generatePoints();
		test_doCluster();
	}
}
