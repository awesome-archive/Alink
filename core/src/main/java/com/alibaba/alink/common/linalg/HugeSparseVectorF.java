package com.alibaba.alink.common.linalg;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Arrays;

public class HugeSparseVectorF implements Serializable {

	private static final long serialVersionUID = 2988250478711230676L;
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
	private float[] values;

	/**
	 * Construct a empty vector
	 */
	public HugeSparseVectorF() {
		this.size = -1L;
		this.indices = new long[0];
		this.values = new float[0];
	}

	/**
	 * Construct a vector with zero nnz
	 *
	 * @param size
	 */
	public HugeSparseVectorF(long size) {
		this.size = size;
		this.indices = new long[0];
		this.values = new float[0];
	}

	/**
	 * Construct a sparse vector.
	 *
	 * @param size
	 * @param indices
	 * @param values
	 * @raise IllegalArgumentException if duplicated indices exist.
	 */
	public HugeSparseVectorF(long size, final long[] indices, final float[] values) {
		if (indices.length != values.length) {
			throw new AkIllegalArgumentException("indices size and values size should be the same.");
		}
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] < 0L || (size >= 0L && indices[i] >= size)) {
				throw new AkIllegalArgumentException("Invalid index: " + indices[i]);
			}
		}
		Tuple2 <long[], float[]> indicesAndValues = HugeSparseVectorCommon.sortIndices(indices, values);
		this.size = size;
		this.indices = indicesAndValues.f0;
		this.values = indicesAndValues.f1;
	}

	/**
	 * Parse a kv string to a sparse vector.
	 *
	 * @param kvString
	 * @param colDelimiter
	 * @param valDelimiter
	 * @return
	 */
	public static HugeSparseVectorF parseKV(String kvString, String colDelimiter, String valDelimiter) {
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
	public static HugeSparseVectorF parseKV(String str, long size, String colDelimiter, String valDelimiter) {
		Tuple2 <long[], float[]> indicesAndValues = HugeSparseVectorCommon.parseKvToSpvF(str, colDelimiter,
			valDelimiter);
		return new HugeSparseVectorF(size, indicesAndValues.f0, indicesAndValues.f1);
	}

	/**
	 * Parse a sparse tensor string to vector
	 *
	 * @param str
	 * @return
	 */
	public static HugeSparseVectorF parseTensor(String str) {
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
		if (str.contains(",")) {
			Tuple2 <long[], float[]> indicesAndValues = HugeSparseVectorCommon.parseKvToSpvF(str, ",", ":");
			return new HugeSparseVectorF(size, indicesAndValues.f0, indicesAndValues.f1);
		} else {
			Tuple2 <long[], float[]> indicesAndValues = HugeSparseVectorCommon.parseKvToSpvF(str, " ", ":");
			return new HugeSparseVectorF(size, indicesAndValues.f0, indicesAndValues.f1);
		}
	}

	/**
	 * Create a new vector by adding a col to the head of this vector
	 *
	 * @param d
	 * @return
	 */
	public static HugeSparseVectorF prefix(HugeSparseVectorF vec, float d) {
		long[] indices = new long[vec.indices.length + 1];
		float[] values = new float[vec.values.length + 1];
		long n = (vec.size >= 0) ? vec.size + 1 : -1L;

		indices[0] = 0;
		values[0] = d;

		for (int i = 0; i < vec.indices.length; i++) {
			indices[i + 1] = vec.indices[i] + 1;
			values[i + 1] = vec.values[i];
		}

		return new HugeSparseVectorF(n, indices, values);
	}

	/**
	 * Create a new vector by appending a col to the end of this vector
	 *
	 * @param d
	 * @return
	 */
	public static HugeSparseVectorF append(HugeSparseVectorF vec, float d) {
		long[] indices = new long[vec.indices.length + 1];
		float[] values = new float[vec.values.length + 1];
		long n = (vec.size >= 0) ? vec.size + 1 : -1L;

		int i;
		for (i = 0; i < vec.indices.length; i++) {
			indices[i] = vec.indices[i];
			values[i] = vec.values[i];
		}

		indices[i] = n - 1;
		values[i] = d;

		return new HugeSparseVectorF(n, indices, values);
	}

	/**
	 * Create a new vector by minus 'this' vector and 'other' vector
	 *
	 * @return
	 */
	public static HugeSparseVectorF minus(HugeSparseVectorF a, HugeSparseVectorF b) {
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

		HugeSparseVectorF r = new HugeSparseVectorF(a.getSize());
		r.indices = new long[totNnz];
		r.values = new float[totNnz];
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
	public static HugeSparseVectorF plus(HugeSparseVectorF a, HugeSparseVectorF b) {
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

		HugeSparseVectorF r = new HugeSparseVectorF(a.getSize());
		r.indices = new long[totNnz];
		r.values = new float[totNnz];
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
	public static HugeSparseVectorF scale(HugeSparseVectorF vec, float d) {
		HugeSparseVectorF r = new HugeSparseVectorF(vec.size, vec.indices, vec.values);
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
	public static float dot(HugeSparseVectorF a, HugeSparseVectorF b) {
		if (a.getSize() != b.getSize()) {
			throw new AkIllegalDataException("the size of the two vectors are different");
		}

		float d = 0;
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

	public float[] getValues() {
		return values;
	}

	public void setValues(float[] values) {
		this.values = values;
	}

	public float sumValue() {
		float d = 0.F;
		for (float t : values) {
			d += t;
		}
		return d;
	}

	public float sumAbsValue() {
		float d = 0.F;
		for (float t : values) {
			d += Math.abs(t);
		}
		return d;
	}

	public float sumSquaredValue() {
		float d = 0.F;
		for (float t : values) {
			d += t * t;
		}
		return d;
	}

	public float maxValue() {
		float d = -Float.MAX_VALUE;
		for (float t : values) {
			d = Math.max(d, t);
		}
		return d;
	}

	public float minValue() {
		float d = Float.MAX_VALUE;
		for (float t : values) {
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
	public float get(long index) {
		int pos = Arrays.binarySearch(indices, index);
		if (pos >= 0) {
			return values[pos];
		}
		return 0.F;
	}

	/**
	 * Update the index-th element of this vector.
	 *
	 * @param index
	 * @param val
	 */
	public void update(long index, float val) {
		int pos = Arrays.binarySearch(indices, index);
		if (pos >= 0) {
			this.values[pos] = val;
		} else {
			throw new AkIllegalDataException("Can't find index: " + index);
		}
	}

	/**
	 * Scale this vector by a factor
	 *
	 * @param d
	 */
	public void scale(float d) {
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] *= d;
		}
	}

	@Override
	public String toString() {
		return "HugeSparseVectorD{" +
			"indices=" + Arrays.toString(indices) +
			"values=" + Arrays.toString(values) +
			"vectorSize=" + size +
			'}';
	}
}
