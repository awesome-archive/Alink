package com.alibaba.alink.common.insights;

import java.util.Map;

public class ColumnName {
	public String colCnName;
	public String colName;

	public ColumnName() {}

	public ColumnName(String colName, Map <String, String> colNameMap) {
		this.colName = colName;
		setColCnName(colName, colNameMap);
	}

	public ColumnName(String colName) {
		this.colName = colName;
		this.colCnName = null;
	}

	public ColumnName(String colName, String colCnName) {
		this.colName = colName;
		this.colCnName = colCnName;
	}

	public String getColCnName() {
		if (null == this.colCnName) {
			return this.colName;
		} else {
			return this.colCnName;
		}
	}

	public void setColCnName(String colName, Map <String, String> colNameMap) {
		if (null == colNameMap || !colNameMap.containsKey(colName)) {
			this.colCnName = null;
		} else {
			this.colCnName = colNameMap.get(colName);
		}
	}

	public void setColCnName(String colCnName) {
		this.colCnName = colCnName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getColName() {
		return this.colName;
	}
}
