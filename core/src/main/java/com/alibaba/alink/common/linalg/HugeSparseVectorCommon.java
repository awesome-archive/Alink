package com.alibaba.alink.common.linalg;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Comparator;

public class HugeSparseVectorCommon {

	public static Tuple2 <long[], double[]> parseKvToSpvD(String str, String colDelimiter, String valDelimiter) {

		if (StringUtils.isBlank(str)) {
			return Tuple2.of(new long[0], new double[0]);
		}

		try {
			int numValues = StringUtils.countMatches(str, colDelimiter) + 1;
			long[] indices = new long[numValues];
			double[] values = new double[numValues];

			int startPos = 0;
			int endPos = -1;
			int delimiterPos;

			for (int i = 0; i < numValues; i++, startPos = endPos + 1) {
				endPos = StringUtils.indexOf(str, colDelimiter, startPos);
				if (endPos == -1) {
					endPos = str.length();
				}
				delimiterPos = StringUtils.indexOf(str, valDelimiter, startPos);
				if (delimiterPos == -1 || delimiterPos >= endPos) {
					throw new AkIllegalDataException("invalid data: " + str);
				}

				// skip white space in the key
				for (; startPos < delimiterPos; startPos++) {
					if (!Character.isWhitespace(str.charAt(startPos))) {
						break;
					}
				}
				if (startPos == delimiterPos) {
					throw new AkIllegalDataException("empty indices : " + str);
				}
				indices[i] = Long.valueOf(StringUtils.substring(str, startPos, delimiterPos));

				// skip white space in the value
				for (; delimiterPos + 1 < endPos; delimiterPos++) {
					if (!Character.isWhitespace(str.charAt(delimiterPos + 1))) {
						break;
					}
				}
				if (delimiterPos + 1 == endPos) {
					throw new AkIllegalDataException("empty values : " + str);
				}
				values[i] = Double.valueOf(StringUtils.substring(str, delimiterPos + 1, endPos));
			}

			return Tuple2.of(indices, values);
		} catch (Exception e) {
			throw new AkIllegalDataException("Fail to convert kv string to sparse vector: " + str, e);
		}
	}

	public static Tuple2 <long[], float[]> parseKvToSpvF(String str, String colDelimiter, String valDelimiter) {

		if (StringUtils.isBlank(str)) {
			return Tuple2.of(new long[0], new float[0]);
		}

		try {
			int numValues = StringUtils.countMatches(str, colDelimiter) + 1;
			long[] indices = new long[numValues];
			float[] values = new float[numValues];

			int startPos = 0;
			int endPos = -1;
			int delimiterPos;

			for (int i = 0; i < numValues; i++, startPos = endPos + 1) {
				endPos = StringUtils.indexOf(str, colDelimiter, startPos);
				if (endPos == -1) {
					endPos = str.length();
				}
				delimiterPos = StringUtils.indexOf(str, valDelimiter, startPos);
				if (delimiterPos == -1 || delimiterPos >= endPos) {
					throw new AkIllegalDataException("invalid data: " + str);
				}

				// skip white space in the key
				for (; startPos < delimiterPos; startPos++) {
					if (!Character.isWhitespace(str.charAt(startPos))) {
						break;
					}
				}
				if (startPos == delimiterPos) {
					throw new AkIllegalDataException("empty indices : " + str);
				}
				indices[i] = Long.valueOf(StringUtils.substring(str, startPos, delimiterPos));

				// skip white space in the value
				for (; delimiterPos + 1 < endPos; delimiterPos++) {
					if (!Character.isWhitespace(str.charAt(delimiterPos + 1))) {
						break;
					}
				}
				if (delimiterPos + 1 == endPos) {
					throw new AkIllegalDataException("empty values : " + str);
				}
				values[i] = Float.valueOf(StringUtils.substring(str, delimiterPos + 1, endPos));
			}

			return Tuple2.of(indices, values);
		} catch (Exception e) {
			throw new AkIllegalDataException("Fail to convert kv string to sparse vector: " + str, e);
		}
	}

	public static Tuple2 <long[], double[]> sortIndices(final long[] indices, final double[] values) {
		boolean outOfOrder = false;
		for (int i = 0; i < indices.length - 1; i++) {
			if (indices[i] >= indices[i + 1]) {
				outOfOrder = true;
				break;
			}
		}

		if (outOfOrder) {
			// sort
			Integer[] order = new Integer[indices.length];
			for (int i = 0; i < order.length; i++) {
				order[i] = i;
			}

			Arrays.sort(order, new Comparator <Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					if (indices[o1] < indices[o2]) { return -1; } else if (indices[o1] > indices[o2]) {
						return 1;
					} else { return 0; }
				}
			});

			int nnz = indices.length;
			long[] indices1 = new long[nnz];
			double[] values1 = new double[nnz];

			for (int i = 0; i < order.length; i++) {
				indices1[i] = indices[order[i]];
				values1[i] = values[order[i]];

				if (i > 0) {
					if (indices1[i] == indices1[i - 1]) {
						throw new AkIllegalArgumentException("Two indices are the same in sparse vector: " +
							indices1[i]);
					}
				}
			}
			return Tuple2.of(indices1, values1);
		} else {
			return Tuple2.of(indices.clone(), values.clone());
		}
	}

	public static Tuple2 <long[], float[]> sortIndices(final long[] indices, final float[] values) {
		boolean outOfOrder = false;
		for (int i = 0; i < indices.length - 1; i++) {
			if (indices[i] >= indices[i + 1]) {
				outOfOrder = true;
				break;
			}
		}

		if (outOfOrder) {
			// sort
			Integer[] order = new Integer[indices.length];
			for (int i = 0; i < order.length; i++) {
				order[i] = i;
			}

			Arrays.sort(order, new Comparator <Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					if (indices[o1] < indices[o2]) { return -1; } else if (indices[o1] > indices[o2]) {
						return 1;
					} else { return 0; }
				}
			});

			int nnz = indices.length;
			long[] indices1 = new long[nnz];
			float[] values1 = new float[nnz];

			for (int i = 0; i < order.length; i++) {
				indices1[i] = indices[order[i]];
				values1[i] = values[order[i]];

				if (i > 0) {
					if (indices1[i] == indices1[i - 1]) {
						throw new AkIllegalDataException("Two indices are the same in sparse vector: " +
							indices1[i]);
					}
				}
			}
			return Tuple2.of(indices1, values1);
		} else {
			return Tuple2.of(indices.clone(), values.clone());
		}
	}

}
