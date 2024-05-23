package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.RMapper;
import com.alibaba.alink.params.dataproc.RUdfParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("RUdfBatchOp")
@NameEn("RUdfBatchOp")
public final class RUdfBatchOp extends MapBatchOp <RUdfBatchOp>
	implements RUdfParams <RUdfBatchOp> {
	private static final long serialVersionUID = -2658623503634689607L;

	public RUdfBatchOp() {
		this(null);
	}

	public RUdfBatchOp(Params params) {
		super(RMapper::new, params);
	}

}
