package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Breakdown extends ColumnName implements Serializable {

	public Breakdown() {

	}

	public Breakdown(String colName) {
		this.colName = colName;
	}
}
