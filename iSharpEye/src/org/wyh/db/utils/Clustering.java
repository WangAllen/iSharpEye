package org.wyh.db.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * 聚类不要再写了，使用mahout库来做。
 * 
 * @author wail
 *
 */
public class Clustering {

	public static HashMap<String, Integer> basicKMeans(String fileName, int k, int minDistance) {

		return null;
	}

	/**
	 * 聚类
	 * 
	 * @param p
	 * @param k
	 *            聚类数
	 * @return
	 */
	public static double[][] cluster(double[] p, int k) {
		// 存放聚类旧的聚类中心
		double[] center = new double[k];
		// 存放新计算的聚类中心
		double[] newCenter = new double[k];
		// 存放放回结果
		double[][] g;
		// 初始化聚类中心
		// 经典方法是随机选取 k 个
		// 本例中采用前 k个作为聚类中心
		// 聚类中心的选取不影响最终结果
		for (int i = 0; i < k; i++) {
			center[i] = p[i];
		}
		// 循环聚类，更新聚类中心
		// 到聚类中心不变为止
		while (true) {
			// 根据聚类中心将元素分类
			g = group(p, center);
			// 计算分类后的聚类中心
			for (int i = 0; i < g.length; i++) {
				newCenter[i] = center(g[i]);
			}
			// 如果聚类中心不同
			if (!equal(newCenter, center)) {
				// 为下一次聚类准备
				center = newCenter;
				newCenter = new double[k];
			} else // 聚类结束
				break;
		}
		// 返回聚类结果
		return g;
	}

	public static double center(double[] p) {
		return sum(p) / p.length;
	}

	public static double[][] group(double[] p, double[] center) {
		// 中间变量，用来分组标记
		int[] gi = new int[p.length];
		// 考察每一个元素 pi 同聚类中心 cj 的距离
		// pi 与 cj 的距离最小则归为 j 类
		for (int i = 0; i < p.length; i++) {
			// 存放距离
			double[] dist = new double[center.length];
			// 计算到每个聚类中心的距离
			for (int j = 0; j < center.length; j++) {
				dist[j] = distance(p[i], center[j]);
			}
			// 找出最小距离
			int ci = min(dist);
			// 标记属于哪一组
			gi[i] = ci;
		}
		// 存放分组结果
		double[][] g = new double[center.length][];
		// 遍历每个聚类中心，分组
		for (int i = 0; i < center.length; i++) {
			// 中间变量，记录聚类后每一组的大小
			int s = 0;
			// 计算每一组的长度
			for (int j = 0; j < gi.length; j++)
				if (gi[j] == i)
					s++;
			// 存储每一组的成员
			g[i] = new double[s];
			s = 0;
			// 根据分组标记将各元素归位
			for (int j = 0; j < gi.length; j++)
				if (gi[j] == i) {
					g[i][s] = p[j];
					s++;
				}
		}
		// 返回分组结果
		return g;
	}

	public static double distance(double x, double y) {
		return Math.abs(x - y);
	}

	public static double sum(double[] p) {
		double sum = 0.0;
		for (int i = 0; i < p.length; i++)
			sum += p[i];
		return sum;
	}

	public static int min(double[] p) {
		int i = 0;
		double m = p[0];
		for (int j = 1; j < p.length; j++) {
			if (p[j] < m) {
				i = j;
				m = p[j];
			}
		}
		return i;
	}

	public static boolean equal(double[] a, double[] b) {
		if (a.length != b.length)
			return false;
		else {
			for (int i = 0; i < a.length; i++) {
				if (a[i] != b[i])
					return false;
			}
		}
		return true;
	}

	/**
	 * 散点聚类
	 * 
	 * @param points
	 * @param k
	 * @return
	 */
	public static double[][][] doCluster(double[][] points, int k) {
		// 存放放回结果
		double[][][] clusters = new double[k][][];

		// 存放聚类旧的聚类中心，二维
		double[][] center = new double[k][2];
		// 存放新计算的聚类中心
		double[][] newCenter = new double[k][2];

		// 初始化聚类中心
		// 经典方法是随机选取 k 个
		// 本例中采用前 k个作为聚类中心
		// 聚类中心的选取不影响最终结果
		for (int i = 0; i < k; i++) {
			center[i] = points[i];
		}
		// 循环聚类，更新聚类中心
		// 到聚类中心不变为止
		while (true) {
			// 根据聚类中心将元素分类
			clusters = doGroup(points, center);
			// 计算分类后的聚类中心
			for (int i = 0; i < clusters.length; i++) {
				newCenter[i] = getCenter(clusters[i]);
			}
			// 如果聚类中心不同
			if (!equal(newCenter, center)) {
				// 为下一次聚类准备
				center = newCenter;
				newCenter = new double[k][2];
			} else {// 聚类结束
				break;
			}
		}
		// 返回聚类结果
		return clusters;
	}

	/**
	 * 查看是否两个数组相等
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private static boolean equal(double[][] a, double[][] b) {
		if (a.length != b.length) {
			return false;
		} else {
			for (int i = 0; i < a.length; i++) {
				if (a[i].length != b[i].length) {
					return false;
				} else {
					for (int j = 0; j < a[i].length; j++)
						if (a[i][j] != b[i][j]) {
							return false;
						}
				}
			}
		}
		return true;
	}

	public static double[][][] doGroup(double[][] points, double[][] centers) {

		// 中间变量，用标记每个点属于哪个组记
		int[] gi = new int[points.length];

		// 考察每一点素 p 同聚类中点 c 的距离
		// p 与 c 的距离最小则归 类
		for (int i = 0; i < points.length; i++) {
			// 存放距离
			double[] dist = new double[centers.length];
			// 计算到每个聚类中心的距离
			for (int j = 0; j < centers.length; j++) {
				dist[j] = getDistance(points[i], centers[j]);
			}
			// 找出最小距离
			int ci = min(dist);
			// 标记属于哪一组
			gi[i] = ci;
		}

		// 存放分组结果
		double[][][] group = new double[centers.length][][];
		// 遍历每个聚类中心，分组
		for (int i = 0; i < centers.length; i++) {
			// 中间变量，记录聚类后每一组的大小
			int s = 0;
			// 计算每一组的长度
			for (int j = 0; j < gi.length; j++) {
				if (gi[j] == i) {
					s++;
				}
			}
			// 存储每一组的成员
			group[i] = new double[s][2];
			s = 0;
			// 根据分组标记将各元素归位
			for (int j = 0; j < gi.length; j++) {
				if (gi[j] == i) {
					group[i][s] = points[j];
					s++;
				}
			}
		}
		// 返回分组结果
		return group;
	}

	private static double getDistance(double[] point1, double[] point2) {

		return DBOperations.getDist(point1[0], point1[1], point2[0], point2[1]);
	}

	public static double[] getCenter(double[][] points) {
		// center是一个数组，有两个元素，分别为x与y坐标
		double[] center = new double[2];
		double x_axis = 0;
		double y_axis = 0;
		for (int i = 0; i < points.length; i++) {
			x_axis += points[i][0];
			y_axis += points[i][1];
		}
		// 以均值为中心坐标
		center[0] = x_axis / points.length;
		center[1] = y_axis / points.length;
		return center;
	}

	/**
	 * 随机产生number个点，范围为：经度(-180,180]，纬度(-90,90]
	 * 
	 * @param number
	 * @return
	 */
	public static double[][] generatePoints(int number) {
		double[][] points = new double[number][2];
		for (int i = 0; i < number; i++) {
			points[i][0] = Math.random() * 360 - 180;
			points[i][1] = Math.random() * 180 - 90;
		}
		return points;
	}

	public static final double[][] points = { { 1, 1 }, { 2, 1 }, { 1, 2 }, { 2, 2 }, { 3, 3 }, { 8, 8 }, { 9, 8 },
			{ 8, 9 }, { 9, 9 }, { 5, 5 }, { 5, 6 }, { 6, 6 } };

	public static List<Vector> getPointVectors(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}

		return points;
	}

	public static List<Vector> generateAppleData() {
		List<Vector> apples = new ArrayList<Vector>();
		NamedVector apple = new NamedVector(new DenseVector(new double[] { 0.11, 510, 1 }), "Small round green apple");
		apples.add(apple);
		apple = new NamedVector(new DenseVector(new double[] {0.2, 650, 3}), "Large oval red apple");
		apples.add(apple);
		apple = new NamedVector(new DenseVector(new double[] {0.25, 590, 3}), "Large round yellow apple");
		apples.add(apple);
		apple = new NamedVector(new DenseVector(new double[] {0.18, 520, 2}), "Medium oval green apple");
		apples.add(apple);
		return apples;
	}
}

/*
 * 平面上有一系列的点points={p1,p2,...,pn}，每个点pi = (lat_i,lon_i) 聚集成为k个类， k<=n
 * 初始化k个中心centers = {c1, c2, ... ,ck}, 每个cj = (lat_j, lon_j)
 * 计算每个点到这k个中心点的距离，距离哪个中心点最近则归属于哪个类 更新中心点：How？ 重新计算每个点到中心点的距离，重复更新中心点，直到中心点不再变化
 */