package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.params.udf.PandasUdfFileParams;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("PandasUdfFilStreamOp")
@NameEn("PandasUdfFilStreamOp")
public class PandasUdfFilStreamOp extends BasePandasUdfStreamOp <PandasUdfFilStreamOp>
	implements PandasUdfFileParams <PandasUdfFilStreamOp> {

	public PandasUdfFilStreamOp() {
		this(null);
	}

	public PandasUdfFilStreamOp(Params param) {
		super(param);
	}
}
