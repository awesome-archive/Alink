package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.params.udf.PandasUdfParams;

@Internal
public final class PandasUdfBatchOp extends BasePandasUdfBatchOp <PandasUdfBatchOp>
	implements PandasUdfParams <PandasUdfBatchOp> {

	public PandasUdfBatchOp() {
		this(null);
	}

	public PandasUdfBatchOp(Params param) {
		super(param);
	}
}
