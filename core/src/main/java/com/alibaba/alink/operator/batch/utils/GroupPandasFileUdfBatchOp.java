package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.params.udf.GroupPandasFileUdfParams;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("GroupPandasFileUdfBatchOp")
@NameEn("GroupPandasFileUdfBatchOp")
public class GroupPandasFileUdfBatchOp extends BaseGroupPandasUdfBatchOp <GroupPandasFileUdfBatchOp>
	implements GroupPandasFileUdfParams <GroupPandasFileUdfBatchOp> {

	public GroupPandasFileUdfBatchOp() {
		this(null);
	}

	public GroupPandasFileUdfBatchOp(Params params) {
		super(params);
	}

}
