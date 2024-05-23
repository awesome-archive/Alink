package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.params.udf.PandasUdfParams;

@Internal
public class PandasUdfStreamOp extends BasePandasUdfStreamOp <PandasUdfStreamOp>
	implements PandasUdfParams <PandasUdfStreamOp> {

	public PandasUdfStreamOp() {
		this(null);
	}

	public PandasUdfStreamOp(Params param) {
		super(param);
	}
}
