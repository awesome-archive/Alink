package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.PandasUdfMapper;
import com.alibaba.alink.params.udf.BasePandasUdfParams;

abstract class BasePandasUdfStreamOp<T extends BasePandasUdfStreamOp <T>> extends MapStreamOp <T>
	implements BasePandasUdfParams <T> {

	public BasePandasUdfStreamOp() {
		this(null);
	}

	public BasePandasUdfStreamOp(Params param) {
		super(PandasUdfMapper::new, param);
	}
}
