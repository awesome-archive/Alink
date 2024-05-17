package com.alibaba.alink.common.linalg;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HugeSparseVectorD implements Serializable {

	private static final long serialVersionUID = 7334516922101728022L;
	/**
	 * size of the vector. -1 indicates undetermined size.
	 */
	private long size;

	/**
	 * column indices
	 */
	private long[] indices;

	/**
	 * column values
	 */
	private double[] values;

	/**
	 * Construct a empty vector
	 */
	public HugeSparseVectorD() {
		this.size = -1L;
		this.indices = new long[0];
		this.values = new double[0];
	}

	/**
	 * Construct a vector with zero nnz
	 *
	 * @param size
	 */
	public HugeSparseVectorD(long size) {
		this.size = size;
		this.indices = new long[0];
		this.values = new double[0];
	}

	public HugeSparseVectorD(final long[] indices, final double[] values) {
		this(-1L, indices, values);
	}

	/**
	 * Construct a sparse vector.
	 *
	 * @param size
	 * @param indices
	 * @param values
	 * @raise IllegalArgumentException if duplicated indices exist.
	 */
	public HugeSparseVectorD(long size, final long[] indices, final double[] values) {
		init(size, indices, values);
	}

	public HugeSparseVectorD(List <Tuple2 <Long, Double>> list) {
		int m = list.size();
		long[] indices = new long[m];
		double[] values = new double[m];
		for (int i = 0; i < m; i++) {
			indices[i] = list.get(i).f0;
			values[i] = list.get(i).f1;
		}
		init(-1L, indices, values);
	}

	/**
	 * Parse a kv string to a sparse vector.
	 *
	 * @param kvString
	 * @param colDelimiter
	 * @param valDelimiter
	 * @return
	 */
	public static HugeSparseVectorD parseKV(String kvString, String colDelimiter, String valDelimiter) {
		return parseKV(kvString, -1L, colDelimiter, valDelimiter);
	}

	/**
	 * * Parse a kv string to a sparse vector.
	 *
	 * @param str
	 * @param size:        the size of the vector. if size == -1, then the size is undetermined
	 * @param colDelimiter
	 * @param valDelimiter
	 * @return
	 */
	public static HugeSparseVectorD parseKV(String str, long size, String colDelimiter, String valDelimiter) {
		Tuple2 <long[], double[]> indicesAndValues = HugeSparseVectorCommon.parseKvToSpvD(str, colDelimiter,
			valDelimiter);
		return new HugeSparseVectorD(size, indicesAndValues.f0, indicesAndValues.f1);
	}

	/**
	 * Parse a sparse tensor string to vector
	 *
	 * @param str
	 * @return
	 */
	public static HugeSparseVectorD parseTensor(String str) {
		int p = StringUtils.indexOf(str, '$');
		long size = -1L;
		if (p >= 0) {
			try {
				int q = StringUtils.lastIndexOf(str, '$');
				size = Long.valueOf(StringUtils.substring(str, p + 1, q));
				str = StringUtils.substring(str, q + 1, str.length());
			} catch (Exception e) {
				throw new AkIllegalDataException("Invalid tensor string: " + str, e);
			}
		}
		Tuple2 <long[], double[]> indicesAndValues = HugeSparseVectorCommon.parseKvToSpvD(str, " ", ":");
		return new HugeSparseVectorD(size, indicesAndValues.f0, indicesAndValues.f1);
	}

	/**
	 * Create a new vector by adding a col to the head of this vector
	 *
	 * @param d
	 * @return
	 */
	public static HugeSparseVectorD prefix(HugeSparseVectorD vec, double d) {
		long[] indices = new long[vec.indices.length + 1];
		double[] values = new double[vec.values.length + 1];
		long n = (vec.size >= 0) ? vec.size + 1 : -1L;

		indices[0] = 0;
		values[0] = d;

		for (int i = 0; i < vec.indices.length; i++) {
			indices[i + 1] = vec.indices[i] + 1;
			values[i + 1] = vec.values[i];
		}

		return new HugeSparseVectorD(n, indices, values);
	}

	/**
	 * Create a new vector by appending a col to the end of this vector
	 *
	 * @param d
	 * @return
	 */
	public static HugeSparseVectorD append(HugeSparseVectorD vec, double d) {
		long[] indices = new long[vec.indices.length + 1];
		double[] values = new double[vec.values.length + 1];
		long n = (vec.size >= 0) ? vec.size + 1 : -1L;

		int i;
		for (i = 0; i < vec.indices.length; i++) {
			indices[i] = vec.indices[i];
			values[i] = vec.values[i];
		}

		indices[i] = n - 1;
		values[i] = d;

		return new HugeSparseVectorD(n, indices, values);
	}

	/**
	 * Create a new vector by minus 'this' vector and 'other' vector
	 *
	 * @return
	 */
	public static HugeSparseVectorD minus(HugeSparseVectorD a, HugeSparseVectorD b) {
		if (a.getSize() != b.getSize()) {
			throw new AkIllegalDataException("the size of the two vectors are different");
		}

		int nnz0 = a.indices.length;
		int nnz1 = b.indices.length;
		int totNnz = nnz0 + nnz1;
		int p0 = 0;
		int p1 = 0;
		while (p0 < nnz0 && p1 < nnz1) {
			if (a.indices[p0] == b.indices[p1]) {
				totNnz--;
				p0++;
				p1++;
			} else if (a.indices[p0] < b.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}

		HugeSparseVectorD r = new HugeSparseVectorD(a.getSize());
		r.indices = new long[totNnz];
		r.values = new double[totNnz];
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < nnz0 && p1 < nnz1) {
				if (a.indices[p0] == b.indices[p1]) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0] - b.values[p1];
					p0++;
					p1++;
				} else if (a.indices[p0] < b.indices[p1]) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0];
					p0++;
				} else {
					r.indices[pos] = b.indices[p1];
					r.values[pos] = -b.values[p1];
					p1++;
				}
				pos++;
			} else {
				if (p0 < nnz0) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < nnz1) {
					r.indices[pos] = b.indices[p1];
					r.values[pos] = -b.values[p1];
					p1++;
					pos++;
					continue;
				}
			}
		}

		return r;
	}

	/**
	 * Create a new vector by adding 'this' vector and 'other' vector
	 *
	 * @return
	 */
	public static HugeSparseVectorD plus(HugeSparseVectorD a, HugeSparseVectorD b) {
		if (a.getSize() != b.getSize()) { throw new AkIllegalDataException("the size of the two vectors are different"); }

		int nnz0 = a.indices.length;
		int nnz1 = b.indices.length;
		int totNnz = nnz0 + nnz1;
		int p0 = 0;
		int p1 = 0;
		while (p0 < nnz0 && p1 < nnz1) {
			if (a.indices[p0] == b.indices[p1]) {
				totNnz--;
				p0++;
				p1++;
			} else if (a.indices[p0] < b.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}

		HugeSparseVectorD r = new HugeSparseVectorD(a.getSize());
		r.indices = new long[totNnz];
		r.values = new double[totNnz];
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < nnz0 && p1 < nnz1) {
				if (a.indices[p0] == b.indices[p1]) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0] + b.values[p1];
					p0++;
					p1++;
				} else if (a.indices[p0] < b.indices[p1]) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0];
					p0++;
				} else {
					r.indices[pos] = b.indices[p1];
					r.values[pos] = b.values[p1];
					p1++;
				}
				pos++;
			} else {
				if (p0 < nnz0) {
					r.indices[pos] = a.indices[p0];
					r.values[pos] = a.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < nnz1) {
					r.indices[pos] = b.indices[p1];
					r.values[pos] = b.values[p1];
					p1++;
					pos++;
					continue;
				}
			}
		}
		return r;
	}

	/**
	 * Create a new vector by scaling 'this' vector with a factor
	 *
	 * @param d
	 * @return
	 */
	public static HugeSparseVectorD scale(HugeSparseVectorD vec, double d) {
		HugeSparseVectorD r = new HugeSparseVectorD(vec.size, vec.indices, vec.values);
		for (int i = 0; i < vec.values.length; i++) {
			r.values[i] *= d;
		}
		return r;
	}

	/**
	 * Return the dot product of this vector and other vector
	 *
	 * @param b
	 * @return
	 */
	public static double dot(HugeSparseVectorD a, HugeSparseVectorD b) {
		if (a.getSize() != b.getSize()) {
			throw new AkIllegalDataException("the size of the two vectors are different");
		}

		double d = 0;
		int p0 = 0;
		int p1 = 0;
		while (p0 < a.indices.length && p1 < b.indices.length) {
			if (a.indices[p0] == b.indices[p1]) {
				d += a.values[p0] * b.values[p1];
				p0++;
				p1++;
			} else if (a.indices[p0] < b.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}
		return d;
	}

	private void init(long size, final long[] indices, final double[] values) {
		if (indices.length != values.length) {
			throw new AkIllegalDataException("indices size and values size should be the same.");
		}
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] < 0L || (size >= 0L && indices[i] >= size)) {
				throw new AkIllegalDataException("Invalid index: " + indices[i]);
			}
		}
		Tuple2 <long[], double[]> indicesAndValues = HugeSparseVectorCommon.sortIndices(indices, values);
		this.size = size;
		this.indices = indicesAndValues.f0;
		this.values = indicesAndValues.f1;
	}

	public List <Tuple2 <Long, Double>> toList() {
		ArrayList <Tuple2 <Long, Double>> res = new ArrayList <>();
		for (int i = 0; i < indices.length; i++) {
			res.add(new Tuple2(indices[i], values[i]));
		}
		return res;
	}

	@Override
	public HugeSparseVectorD clone() {
		HugeSparseVectorD vec = new HugeSparseVectorD(this.size);
		vec.indices = this.indices.clone();
		vec.values = this.values.clone();
		return vec;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] >= size) {
				throw new AkIllegalArgumentException("Vector size <= index: " + indices[i]);
			}
		}
		this.size = size;
	}

	public long[] getIndices() {
		return indices;
	}

	public void setIndices(long[] indices) {
		this.indices = indices;
	}

	public double[] getValues() {
		return values;
	}

	public void setValues(double[] values) {
		this.values = values;
	}

	public double sumValue() {
		double d = 0.;
		for (double t : values) {
			d += t;
		}
		return d;
	}

	public double sumAbsValue() {
		double d = 0.;
		for (double t : values) {
			d += Math.abs(t);
		}
		return d;
	}

	public double sumSquaredValue() {
		double d = 0.;
		for (double t : values) {
			d += t * t;
		}
		return d;
	}

	public double maxValue() {
		double d = -Double.MAX_VALUE;
		for (double t : values) {
			d = Math.max(d, t);
		}
		return d;
	}

	public double minValue() {
		double d = Double.MAX_VALUE;
		for (double t : values) {
			d = Math.min(d, t);
		}
		return d;
	}

	public long maxIndex() {
		if (indices.length > 0) {
			return indices[indices.length - 1];
		} else {
			return -1L;
		}
	}

	public long minIndex() {
		if (indices.length > 0) {
			return indices[0];
		} else {
			return -1L;
		}
	}

	/**
	 * Get the index-th element of this vector
	 *
	 * @param index
	 * @return
	 */
	public double get(long index) {
		int pos = Arrays.binarySearch(indices, index);
		if (pos >= 0) {
			return values[pos];
		}
		return 0.;
	}

	/**
	 * Update the index-th element of this vector.
	 *
	 * @param index
	 * @param val
	 */
	public void update(long index, double val) {
		int pos = Arrays.binarySearch(indices, index);
		if (pos >= 0) {
			this.values[pos] = val;
		} else {
			throw new AkIllegalArgumentException("Can't find index: " + index);
		}
	}

	/**
	 * Scale this vector by a factor
	 *
	 * @param d
	 */
	public HugeSparseVectorD scale(double d) {
		HugeSparseVectorD vec = this.clone();
		for (int i = 0; i < vec.values.length; i++) {
			vec.values[i] *= d;
		}
		return vec;
	}

	public HugeSparseVectorD plus(HugeSparseVectorD vec) {
		return plus(this, vec);
	}

	public double dot(HugeSparseVectorD vec) {
		return dot(this, vec);
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		if (this.size >= 0) {
			sbd.append("$").append(this.size).append("$");
		}
		int len = indices.length;
		for (int i = 0; i < len; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(indices[i]).append(":").append(values[i]);
		}
		return sbd.toString();
	}
}
