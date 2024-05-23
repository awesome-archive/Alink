package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.params.udf.GroupPandasUdfParams;

@Internal
public class GroupPandasUdfBatchOp extends BaseGroupPandasUdfBatchOp <GroupPandasUdfBatchOp>
	implements GroupPandasUdfParams <GroupPandasUdfBatchOp> {

	public GroupPandasUdfBatchOp() {
		this(null);
	}

	public GroupPandasUdfBatchOp(Params params) {
		super(params);
	}

}
