package org.wyh.db.utils;

public class CalcUtils {

	/**
	 * 产生一个不大于num的随机整数
	 * @param num
	 * @return
	 */
	public static long randomNumber(long num) {
		return (long)Math.floor(Math.random() * num);
	}
	
	/**
	 * 产生count个不大于number的随机整数
	 * @param number
	 * @param count
	 * @return
	 */
	public static long[] randomNumbers(long number, int count) {
		long[] nums = new long[count];
		for (int i=0; i<count; i++) {
			nums[i] = (long)Math.floor(Math.random() * number);
		}		
		return nums;		
	}
}
