package com.liukedun;

import java.util.List;

public class Sorter {
	/*
	 * starts with quickSort(list, 1, list.size())
	 */
	public static void quickSort(List<Long> values, int startIndex, int endIndex) {
		if (startIndex < endIndex) {
			int pivot = quickSortPartition(values, startIndex, endIndex);
			quickSort(values, startIndex, pivot - 1);
			quickSort(values, pivot + 1, endIndex);
		}
	}

	public static int quickSortPartition(List<Long> values, int startIndex, int endIndex) {
		double x = values.get(endIndex-1);
		int i = startIndex - 1;
		for (int j = startIndex; j < endIndex; j++) {
			if (values.get(j-1) <= x) {
				i++;
				long tmp = values.get(i-1);
				values.set(i-1, values.get(j-1));
				values.set(j-1, tmp);
			}
		}
		long tmp = values.get(i);
		values.set(i, values.get(endIndex-1));
		values.set(endIndex-1, tmp);
		return i+1;
	}
}
