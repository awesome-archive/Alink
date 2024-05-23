package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.dataproc.RMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.RUdfParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("RUdfStreamOp")
@NameEn("RUdfStreamOp")
public final class RUdfStreamOp extends MapStreamOp <RUdfStreamOp>
	implements RUdfParams <RUdfStreamOp> {
	private static final long serialVersionUID = -2658623503634689607L;

	public RUdfStreamOp() {
		this(null);
	}

	public RUdfStreamOp(Params params) {
		super(RMapper::new, params);
	}

}
