package com.alibaba.alink.operator.batch.mf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.HugeMfAlsImplForHotPoint;
import com.alibaba.alink.params.recommendation.MfAlsParams;

/**
 * Matrix factorization using ALS algorithrim. Support both implicit and explicit model.
 */

@Internal
@InputPorts(values = {
	@PortSpec(PortType.DATA)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.USER_FACTOR),
	@PortSpec(value = PortType.DATA, desc = PortDesc.ITEM_FACTOR)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("支持热点的矩阵分解")
public final class MfAlsForHotPointBatchOp extends BatchOperator <MfAlsForHotPointBatchOp>
	implements MfAlsParams <MfAlsForHotPointBatchOp> {

	private static final long serialVersionUID = 3116407570842047714L;

	public MfAlsForHotPointBatchOp() {
		this(new Params());
	}

	public MfAlsForHotPointBatchOp(Params params) {
		super(params);
	}

	@Override
	public MfAlsForHotPointBatchOp linkFrom(BatchOperator <?>... inputs) {
		boolean implicit = getParams().get(IMPLICIT_PREFS);
		Tuple2 <BatchOperator, BatchOperator> factors = HugeMfAlsImplForHotPoint
			.factorize(inputs[0], getParams(), implicit);

		this.setOutputTable(factors.f0.getOutputTable());
		this.setSideOutputTables(new Table[] {factors.f1.getOutputTable()});
		return this;
	}
}
