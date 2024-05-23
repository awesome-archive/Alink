package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.dataproc.PandasUdfMapper;
import com.alibaba.alink.params.udf.BasePandasUdfParams;

@Internal
abstract class BasePandasUdfBatchOp<T extends BasePandasUdfBatchOp <T>> extends MapBatchOp <T>
	implements BasePandasUdfParams <T> {

	private static final long serialVersionUID = -3686159833821360145L;

	public BasePandasUdfBatchOp() {
		this(null);
	}

	public BasePandasUdfBatchOp(Params param) {
		super(PandasUdfMapper::new, param);
	}
}
