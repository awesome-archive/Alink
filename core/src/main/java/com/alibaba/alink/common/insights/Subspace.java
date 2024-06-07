package com.alibaba.alink.common.insights;

import java.io.Serializable;
import java.util.Map;

public class Subspace extends ColumnName implements Serializable {

	public Object value;

	public Subspace(String colName, Object value) {
		this.colName = colName;
		this.value = value;
	}

	/**
	 * for whole table.
	 */
	public Subspace() {
		this.colName = null;
	}

	@Override
	public String toString() {
		return colName == null ? "" : colName + "=" + value;
	}

	public String strInDescription() {
		return String.format("筛选满足%s=%s条件的数据，", this.getColCnName(), this.value);
	}
}
